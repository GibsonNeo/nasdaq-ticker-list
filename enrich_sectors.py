# =========================
# CONFIG
# =========================

INPUT_CSV = "tickers_clean.csv"
OUTPUT_CSV = "clean_tickers_enriched.csv"
CACHE_DIR = ".cache_ticker_refs"

USE_FINANCEDATABASE = True
USE_SP500_GICS = True
USE_SECTOR_HELPER = True
USE_YFINANCE = True

DROP_MISSING_SECTOR_INDUSTRY = True

# =========================
# CODE
# =========================

from pathlib import Path
import inspect
import pandas as pd
import requests


def read_csv_flex(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=True, na_values=["", "NA", "NaN"])


def norm(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    return s if s else None


def norm_lower(s):
    s = norm(s)
    return s.lower() if s else None


def download_csv(url: str, cache_dir: Path, filename: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    out = cache_dir / filename
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out.write_bytes(r.content)
    return out


def symbol_variants(sym: str):
    s = str(sym).upper().strip()
    if not s:
        return []
    variants = [s]

    if "." in s:
        variants.append(s.replace(".", "-"))

    if " " in s:
        variants.append(s.replace(" ", ""))

    seen = set()
    out = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def normalize_sector_to_allowed(value: str, allowed_sectors):
    v = norm_lower(value)
    if not v:
        return None

    allowed_norm = {norm_lower(a): a for a in allowed_sectors if norm(a)}

    if v in allowed_norm:
        return allowed_norm[v]

    keyword_map = [
        (["technology", "tech", "software", "semiconductor", "it services", "information technology"], "Technology"),
        (["financial", "finance", "bank", "insurance", "capital markets", "asset management", "shell companies"], "Finance"),
        (["health", "health care", "healthcare", "biotech", "pharmaceutical", "medical"], "Health Care"),
        (["industrial", "industrials", "aerospace", "defense", "machinery", "transportation", "construction"], "Industrials"),
        (["energy", "oil", "gas", "coal", "pipelines"], "Energy"),
        (["utility", "utilities", "electric", "water", "gas utilities"], "Utilities"),
        (["telecom", "telecommunications", "communication services"], "Telecommunications"),
        (["real estate", "reit", "property"], "Real Estate"),
        (["consumer discretionary", "discretionary", "retail", "automotive", "apparel", "restaurants"], "Consumer Discretionary"),
        (["consumer staples", "staples", "food", "beverage", "household"], "Consumer Staples"),
        (["basic materials", "materials", "mining", "chemicals", "metals", "forest"], "Basic Materials"),
    ]

    for keys, target in keyword_map:
        if any(k in v for k in keys):
            for a in allowed_sectors:
                if norm_lower(a) == norm_lower(target):
                    return a

    v_tokens = set([t for t in v.replace("&", " ").replace("/", " ").split() if t])
    best = None
    best_score = 0
    for a in allowed_sectors:
        an = norm_lower(a)
        if not an:
            continue
        a_tokens = set([t for t in an.replace("&", " ").replace("/", " ").split() if t])
        score = len(v_tokens.intersection(a_tokens))
        if score > best_score:
            best_score = score
            best = a

    if best_score >= 1:
        return best

    return None


def enrich_from_reference(
    df_target: pd.DataFrame,
    ref_df: pd.DataFrame,
    allowed_sectors,
    allow_new_industry: bool = True,
):
    ref_df = ref_df.dropna(subset=["Symbol"]).copy()
    ref_df["Symbol"] = ref_df["Symbol"].astype(str).str.upper().str.strip()
    ref_df = ref_df.drop_duplicates(subset=["Symbol"], keep="first")
    ref_map = ref_df.set_index("Symbol")

    filled_cells = 0
    allowed_sector_norm = {norm_lower(s) for s in allowed_sectors}

    for idx, row in df_target.iterrows():
        sector_missing = pd.isna(row["Sector"]) or not str(row["Sector"]).strip()
        industry_missing = pd.isna(row["Industry"]) or not str(row["Industry"]).strip()
        if not (sector_missing or industry_missing):
            continue

        sym = str(row["Symbol"]).upper().strip()
        if sym not in ref_map.index:
            continue

        ref = ref_map.loc[sym]
        ref_sector_raw = ref.get("Sector")
        ref_industry_raw = ref.get("Industry")

        mapped_sector = normalize_sector_to_allowed(ref_sector_raw, allowed_sectors)

        if sector_missing and mapped_sector:
            df_target.at[idx, "Sector"] = mapped_sector
            filled_cells += 1

        sector_now = str(df_target.at[idx, "Sector"]).strip() if pd.notna(df_target.at[idx, "Sector"]) else ""
        sector_ok = bool(sector_now) and norm_lower(sector_now) in allowed_sector_norm

        if industry_missing and sector_ok and allow_new_industry:
            ind = norm(ref_industry_raw)
            if ind:
                df_target.at[idx, "Industry"] = ind
                filled_cells += 1

    return filled_cells


def build_ref_from_financedatabase(symbols_needed):
    import financedatabase as fd

    e = fd.Equities()

    try:
        sig = inspect.signature(e.select)
        param_names = list(sig.parameters.keys())
    except Exception:
        sig = None
        param_names = []

    def call_select(v):
        try:
            return e.select(v)
        except Exception:
            pass

        for key in ["symbol", "ticker", "symbols"]:
            if key in param_names:
                try:
                    return e.select(**{key: v})
                except Exception:
                    continue

        return None

    rows = []

    for sym in symbols_needed:
        orig = str(sym).upper().strip()
        if not orig:
            continue

        item = None

        for v in symbol_variants(orig):
            result = call_select(v)

            if not result and hasattr(e, "search"):
                try:
                    result = e.search(v)
                except Exception:
                    result = None

            if not result:
                continue

            if isinstance(result, dict):
                if v in result:
                    item = result[v]
                else:
                    for k, val in result.items():
                        if str(k).upper().strip() == v:
                            item = val
                            break
            elif isinstance(result, list) and result:
                item = result[0]
            else:
                item = result

            if isinstance(item, dict):
                sector = item.get("sector") or item.get("Sector")
                industry = item.get("industry") or item.get("Industry")
                rows.append({"Symbol": orig, "Sector": sector, "Industry": industry})
                break

    ref = pd.DataFrame(rows)
    if ref.empty:
        return None

    ref["Sector"] = ref["Sector"].apply(norm)
    ref["Industry"] = ref["Industry"].apply(norm)
    return ref


def build_ref_from_yfinance(symbols_needed):
    import yfinance as yf

    rows = []
    for sym in symbols_needed:
        orig = str(sym).upper().strip()
        if not orig:
            continue

        info = None

        for v in symbol_variants(orig):
            try:
                info = yf.Ticker(v).info
            except Exception:
                info = None

            if isinstance(info, dict) and info:
                break

        if not isinstance(info, dict) or not info:
            continue

        sector = info.get("sector")
        industry = info.get("industry")
        rows.append({"Symbol": orig, "Sector": sector, "Industry": industry})

    ref = pd.DataFrame(rows)
    if ref.empty:
        return None

    ref["Sector"] = ref["Sector"].apply(norm)
    ref["Industry"] = ref["Industry"].apply(norm)
    return ref


def count_missing(df: pd.DataFrame):
    missing_sector = int((df["Sector"].isna() | (df["Sector"].astype(str).str.strip() == "")).sum())
    missing_industry = int((df["Industry"].isna() | (df["Industry"].astype(str).str.strip() == "")).sum())
    missing_either = int(
        (
            (df["Sector"].isna() | (df["Sector"].astype(str).str.strip() == ""))
            | (df["Industry"].isna() | (df["Industry"].astype(str).str.strip() == ""))
        ).sum()
    )
    return missing_sector, missing_industry, missing_either


def main():
    input_path = Path(INPUT_CSV)
    output_path = Path(OUTPUT_CSV)
    cache_dir = Path(CACHE_DIR)

    df = read_csv_flex(input_path)

    required = {"Symbol", "Sector", "Industry"}
    missing = required.difference(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {sorted(missing)}")

    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()

    needs = (
        df["Sector"].isna() | (df["Sector"].astype(str).str.strip() == "")
        | df["Industry"].isna() | (df["Industry"].astype(str).str.strip() == "")
    )

    allowed_sectors = [v for v in df["Sector"].dropna().unique().tolist() if norm(v)]

    df_work = df.loc[needs].copy()

    print(f"Rows total: {len(df)}")
    print(f"Rows needing enrichment: {len(df_work)}")

    ms, mi, me = count_missing(df)
    print(f"Rows missing Sector: {ms}")
    print(f"Rows missing Industry: {mi}")
    print(f"Rows missing either: {me}")

    if len(df_work) == 0:
        if DROP_MISSING_SECTOR_INDUSTRY:
            before = len(df)
            df = df[
                ~(
                    (df["Sector"].isna() | (df["Sector"].astype(str).str.strip() == ""))
                    | (df["Industry"].isna() | (df["Industry"].astype(str).str.strip() == ""))
                )
            ].copy()
            removed = before - len(df)
            print(f"Final drop stage removed rows: {removed}")

        df.to_csv(output_path, index=False)
        print(f"No missing values, wrote: {output_path}")
        return

    filled_total = 0
    symbols_needed = df_work["Symbol"].dropna().unique().tolist()

    if USE_FINANCEDATABASE:
        try:
            fin_ref = build_ref_from_financedatabase(symbols_needed)
            if fin_ref is None:
                print("Phase 2 FinanceDatabase found no matches")
            else:
                filled = enrich_from_reference(df_work, fin_ref, allowed_sectors, allow_new_industry=True)
                filled_total += filled
                print(f"Phase 2 FinanceDatabase filled cells: {filled}")
                print(f"Phase 2 FinanceDatabase symbols matched: {int(fin_ref['Symbol'].nunique())}")
        except Exception as e:
            print(f"Phase 2 FinanceDatabase skipped, reason: {e}")
    else:
        print("Phase 2 FinanceDatabase disabled")

    if USE_SP500_GICS:
        sp500_url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        try:
            sp500_path = download_csv(sp500_url, cache_dir, "sp500_constituents.csv")
            sp = read_csv_flex(sp500_path)
            ref = pd.DataFrame(
                {
                    "Symbol": sp["Symbol"],
                    "Sector": sp.get("GICS Sector"),
                    "Industry": sp.get("GICS Sub-Industry"),
                }
            )
            filled = enrich_from_reference(df_work, ref, allowed_sectors, allow_new_industry=True)
            filled_total += filled
            print(f"Phase 3 S&P500 GICS filled cells: {filled}")
        except Exception as e:
            print(f"Phase 3 S&P500 GICS skipped, reason: {e}")
    else:
        print("Phase 3 S&P500 GICS disabled")

    if USE_SECTOR_HELPER:
        sector_url = "https://raw.githubusercontent.com/ichoyjx/us_ticker_sectors/main/ticker_sectors.csv"
        try:
            sector_path = download_csv(sector_url, cache_dir, "ticker_sectors.csv")
            ts = read_csv_flex(sector_path)

            if "ticker" in [c.lower() for c in ts.columns]:
                sym_col = [c for c in ts.columns if c.lower() == "ticker"][0]
            else:
                sym_col = "Ticker"

            if "sector" in [c.lower() for c in ts.columns]:
                sec_col = [c for c in ts.columns if c.lower() == "sector"][0]
            else:
                sec_col = None

            if sec_col:
                ref = pd.DataFrame({"Symbol": ts[sym_col], "Sector": ts[sec_col], "Industry": None})
                filled = enrich_from_reference(df_work, ref, allowed_sectors, allow_new_industry=False)
                filled_total += filled
                print(f"Phase 4 sector only mapping filled cells: {filled}")
            else:
                print("Phase 4 sector only mapping skipped, no sector column found")
        except Exception as e:
            print(f"Phase 4 sector only mapping skipped, reason: {e}")
    else:
        print("Phase 4 sector only mapping disabled")

    if USE_YFINANCE:
        try:
            yf_ref = build_ref_from_yfinance(symbols_needed)
            if yf_ref is None:
                print("Phase 5 yfinance found no matches")
            else:
                filled = enrich_from_reference(df_work, yf_ref, allowed_sectors, allow_new_industry=True)
                filled_total += filled
                print(f"Phase 5 yfinance filled cells: {filled}")
                print(f"Phase 5 yfinance symbols matched: {int(yf_ref['Symbol'].nunique())}")
        except Exception as e:
            print(f"Phase 5 yfinance skipped, reason: {e}")
    else:
        print("Phase 5 yfinance disabled")

    df.loc[df_work.index, ["Sector", "Industry"]] = df_work[["Sector", "Industry"]]

    ms2, mi2, me2 = count_missing(df)
    print(f"Total filled cells across phases: {filled_total}")
    print(f"Rows missing Sector after: {ms2}")
    print(f"Rows missing Industry after: {mi2}")
    print(f"Rows missing either after: {me2}")

    if DROP_MISSING_SECTOR_INDUSTRY:
        before = len(df)
        drop_mask = (
            (df["Sector"].isna() | (df["Sector"].astype(str).str.strip() == ""))
            | (df["Industry"].isna() | (df["Industry"].astype(str).str.strip() == ""))
        )
        df = df[~drop_mask].copy()
        removed = before - len(df)
        print(f"Final drop stage removed rows: {removed}")

    df.to_csv(output_path, index=False)
    print(f"Wrote output: {output_path}")


if __name__ == "__main__":
    main()

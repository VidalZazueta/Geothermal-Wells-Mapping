import os
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


BASE_URL = "https://geosteam.conservation.ca.gov"

INPUT_CSV  = "Data/Raw/geothermal_wells_ca.csv"
OUTPUT_CSV = "Data/Processed/geothermal_wells_with_dates.csv"

API_COLUMN = "APINumber"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    )
}

session = requests.Session()
session.headers.update(HEADERS)


def normalize_api(value):
    """Normalize API number to 8 digits by left-padding with zeros."""
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value).strip())
    if not digits:
        return None
    return digits.zfill(8)


def get_with_retry(url, params=None, retries=3):
    """GET request with exponential backoff retry on failure."""
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    raise last_exc


def get_detail_url(api_number):
    """Construct the well detail URL directly from the API number."""
    return f"{BASE_URL}/GeoWellSearch/WellDetails?apinum={api_number}"


def extract_year_drilled(detail_html):
    """Parse the year drilled from a well detail page."""
    soup = BeautifulSoup(detail_html, "html.parser")

    # Primary: <li> elements containing "Year Drilled"
    for li in soup.find_all("li"):
        text = li.get_text(" ", strip=True)
        if "Year Drilled" in text:
            b = li.find("b")
            if b:
                value = b.get_text(strip=True)
                return value if value else None
            m = re.search(r"Year Drilled[:\s]+(\d+)", text)
            if m:
                return m.group(1)

    # Secondary: table rows
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            label = cells[0].get_text(" ", strip=True).lower()
            value = cells[1].get_text(" ", strip=True)
            if "year drilled" in label or "date drilled" in label:
                return value if value else None

    # Tertiary: plain text fallback
    full_text = soup.get_text("\n", strip=True)
    m = re.search(r"Year\s*Drilled[:\s]+(\d+)", full_text, re.IGNORECASE)
    if m:
        return m.group(1)

    return None


def fetch_year_drilled(api_number):
    """Fetch the year drilled for a single API number from the website."""
    time.sleep(2)  # rate-limit: one request every 2 seconds
    r = get_with_retry(get_detail_url(api_number))
    year_drilled = extract_year_drilled(r.text)
    if not year_drilled:
        return None, "year_not_found"
    return year_drilled, "ok"


def save_output(df, cache, path):
    """Create or update the output CSV with scraped year values."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        out = pd.read_csv(path, dtype={API_COLUMN: str})
        out["api_norm"] = out[API_COLUMN].apply(normalize_api)
        # Cast to object so string years can be assigned without dtype errors
        out["year_drilled"] = out["year_drilled"].astype(object) if "year_drilled" in out.columns else pd.Series([None] * len(out), dtype=object)
        out["year_drilled_status"] = out["year_drilled_status"].astype(object) if "year_drilled_status" in out.columns else pd.Series([None] * len(out), dtype=object)
        for idx, row in out.iterrows():
            key = row["api_norm"]
            if pd.notna(key) and key in cache:
                out.at[idx, "year_drilled"] = cache[key][0]
                out.at[idx, "year_drilled_status"] = cache[key][1]
    else:
        out = df.copy()
        out["year_drilled"] = pd.Series(
            [cache.get(x, (None, None))[0] if pd.notna(x) else None for x in out["api_norm"]],
            dtype=object
        )
        out["year_drilled_status"] = pd.Series(
            [cache.get(x, (None, None))[1] if pd.notna(x) else None for x in out["api_norm"]],
            dtype=object
        )

    out.to_csv(path, index=False)
    return out


def load_wells(input_csv=INPUT_CSV):
    """Load the wells CSV and normalize API numbers."""
    df = pd.read_csv(input_csv)
    if API_COLUMN not in df.columns:
        raise ValueError(f"Column '{API_COLUMN}' not found. Available: {list(df.columns)}")
    df["api_norm"] = df[API_COLUMN].apply(normalize_api)
    return df


def load_cache(output_csv=OUTPUT_CSV):
    """Load already-scraped results from the output CSV into a cache dict."""
    cache = {}
    if os.path.exists(output_csv):
        done_df = pd.read_csv(output_csv, dtype={API_COLUMN: str})
        done_df["api_norm"] = done_df[API_COLUMN].apply(normalize_api)
        for _, row in done_df.iterrows():
            key = row["api_norm"]
            if pd.notna(key) and pd.notna(row.get("year_drilled_status")):
                cache[key] = (row.get("year_drilled"), row.get("year_drilled_status"))
    return cache


def scrape_years(df, cache, output_csv=OUTPUT_CSV, checkpoint_every=50):
    """
    Scrape year drilled for all wells not already in cache.
    Saves a checkpoint every `checkpoint_every` wells.
    Returns the updated cache.
    """
    unique_apis = [a for a in df["api_norm"].dropna().unique() if a not in cache]
    total = len(unique_apis)
    print(f"APIs left to fetch: {total}")

    ok = sum(1 for v in cache.values() if v[1] == "ok")
    not_found = sum(1 for v in cache.values() if v[1] == "year_not_found")
    errors = sum(1 for v in cache.values() if v[1] == "error")

    bar = tqdm(unique_apis, desc="Scraping wells", unit="well", dynamic_ncols=True)
    for i, api in enumerate(unique_apis, start=1):
        try:
            year_value, status = fetch_year_drilled(api)
            cache[api] = (year_value, status)
            if status == "ok":
                ok += 1
            else:
                not_found += 1
        except Exception as e:
            cache[api] = (None, "error")
            errors += 1
            tqdm.write(f"  ERROR API {api}: {e}")

        bar.update(1)
        bar.set_postfix(ok=ok, not_found=not_found, errors=errors)

        if i % checkpoint_every == 0:
            save_output(df, cache, output_csv)
            tqdm.write(f"  Checkpoint saved at {i}/{total} wells.")

    bar.close()
    save_output(df, cache, output_csv)
    print(f"\nDone. ok={ok}, not_found={not_found}, errors={errors}")
    print(f"Saved to {output_csv}")
    return cache
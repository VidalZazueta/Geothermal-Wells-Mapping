"""
Web scraping module for retrieving the year each geothermal well was drilled.

Fetches well detail pages from the California GeoSteam website
(https://geosteam.conservation.ca.gov) using each well's API number,
parses the year-drilled value from the HTML, and writes results to a
processed CSV. Includes a cache layer so previously scraped wells are
not re-requested on subsequent runs.
"""

import os
import re
import sys
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from scripts.load_data import load_raw_data


BASE_URL = "https://geosteam.conservation.ca.gov"

INPUT_CSV  = "Data/Raw/geothermal_wells_ca.csv"
OUTPUT_CSV = "Data/Processed/geothermal_wells_cleaned_with_dates.csv"

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
    """
    Normalize an API number to a consistent 8-digit string.

    Strips all non-digit characters from the value, then left-pads
    with zeros until the result is 8 characters long.

    Args:
        value: The raw API number (string, int, float, or NaN).

    Returns:
        str or None: The 8-digit zero-padded API number, or None if
                     the input is NaN or contains no digits.
    """
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value).strip())
    if not digits:
        return None
    return digits.zfill(8)


def get_with_retry(url, params=None, retries=3):
    """
    Perform an HTTP GET request with exponential backoff on failure.

    Retries the request up to `retries` times, waiting 1 s, 2 s, 4 s, …
    between attempts. Raises the last exception if all attempts fail.

    Args:
        url (str): The URL to request.
        params (dict, optional): Query parameters to append to the URL.
        retries (int): Maximum number of attempts. Defaults to 3.

    Returns:
        requests.Response: The successful HTTP response.

    Raises:
        requests.RequestException: If all retry attempts fail.
    """
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
    """
    Build the GeoSteam well-detail page URL for a given API number.

    Args:
        api_number (str): The 8-digit normalized API number.

    Returns:
        str: The full URL to the well's detail page on the GeoSteam website.
    """
    return f"{BASE_URL}/GeoWellSearch/WellDetails?apinum={api_number}"


def extract_year_drilled(detail_html):
    """
    Parse the year a well was drilled from the raw HTML of its detail page.

    Tries three extraction strategies in order:
        1. Looks for a <li> element containing "Year Drilled" and reads the
           value from a <b> tag or a regex match.
        2. Scans <tr> rows for a header cell labelled "year drilled" or
           "date drilled" and reads the adjacent cell.
        3. Falls back to a plain-text regex search over the full page body.

    Args:
        detail_html (str): The raw HTML content of a well detail page.

    Returns:
        str or None: The year drilled as a string (e.g. "1982"), or None
                     if it could not be found.
    """
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
    """
    Fetch the year drilled for a single well from the GeoSteam website.

    Waits 1 seconds before each request to respect the site's rate limit,
    then downloads the well's detail page and extracts the year drilled.

    Args:
        api_number (str): The 8-digit normalized API number for the well.

    Returns:
        tuple: A (year, status) pair where:
            - year (str or None): The year the well was drilled, or None.
            - status (str): "ok" if a year was found, "year_not_found" otherwise.
    """
    time.sleep(1)  # rate-limit: one request every 1 second
    r = get_with_retry(get_detail_url(api_number))
    year_drilled = extract_year_drilled(r.text)
    if not year_drilled:
        return None, "year_not_found"
    return year_drilled, "ok"


def save_output(df, cache, path):
    """
    Write scraped year-drilled values to the output CSV.

    If the output file already exists, it is loaded and the year_drilled /
    year_drilled_status columns are updated in place from the cache.
    If it does not exist, a new file is created from `df` with those columns
    populated from the cache.

    Args:
        df (pandas.DataFrame): The full wells DataFrame with an "api_norm" column.
                               Used as the base when creating the output file for
                               the first time.
        cache (dict): Mapping of normalized API number (str) to a
                      (year, status) tuple produced by fetch_year_drilled.
        path (str): File path where the output CSV should be written.

    Returns:
        pandas.DataFrame: The updated DataFrame that was saved to disk.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        out = load_raw_data(path, dtype={API_COLUMN: str})
        out["API Number Normalized"] = out[API_COLUMN].apply(normalize_api)
        # Cast to object so string years can be assigned without dtype errors
        out["Year Drilled"] = out["Year Drilled"].astype(object) if "Year Drilled" in out.columns else pd.Series([None] * len(out), dtype=object)
        out["Year Drilled Status"] = out["Year Drilled Status"].astype(object) if "Year Drilled Status" in out.columns else pd.Series([None] * len(out), dtype=object)
        for idx, row in out.iterrows():
            key = row["API Number Normalized"]
            if pd.notna(key) and key in cache:
                out.at[idx, "Year Drilled"] = cache[key][0]
                out.at[idx, "Year Drilled Status"] = cache[key][1]
    else:
        out = df.copy()
        out["Year Drilled"] = pd.Series(
            [cache.get(x, (None, None))[0] if pd.notna(x) else None for x in out["API Number Normalized"]],
            dtype=object
        )
        out["Year Drilled Status"] = pd.Series(
            [cache.get(x, (None, None))[1] if pd.notna(x) else None for x in out["API Number Normalized"]],
            dtype=object
        )

    out.to_csv(path, index=False)
    return out


def load_wells(input_csv=INPUT_CSV):
    """
    Load the raw wells CSV and add a normalized API number column.

    Reads the CSV at `input_csv`, verifies that the APINumber column
    is present, and adds an "API Number Normalized" column containing the 8-digit
    zero-padded API number for each row.

    Args:
        input_csv (str): Path to the raw wells CSV file.
                         Defaults to INPUT_CSV ("Data/Raw/geothermal_wells_ca.csv").

    Returns:
        pandas.DataFrame: The wells DataFrame with the "API Number Normalized" column added.

    Raises:
        ValueError: If the APINumber column is not found in the CSV.
    """
    df = load_raw_data(input_csv)
    if API_COLUMN not in df.columns:
        raise ValueError(f"Column '{API_COLUMN}' not found. Available: {list(df.columns)}")
    df["API Number Normalized"] = df[API_COLUMN].apply(normalize_api)
    return df


def load_cache(output_csv=OUTPUT_CSV):
    """
    Build an in-memory cache of already-scraped well results.

    Reads the processed output CSV (if it exists) and returns a dictionary
    mapping each normalized API number to its previously scraped result.
    Wells without a recorded status are excluded so they will be re-scraped.

    Args:
        output_csv (str): Path to the processed output CSV file.
                          Defaults to OUTPUT_CSV ("Data/Processed/geothermal_wells_with_dates.csv").

    Returns:
        dict: Mapping of normalized API number (str) to a (year, status) tuple,
              e.g. {"01234567": ("1982", "ok"), "00987654": (None, "year_not_found")}.
              Returns an empty dict if the output file does not yet exist.
    """
    cache = {}
    if os.path.exists(output_csv):
        done_df = load_raw_data(output_csv, dtype={API_COLUMN: str})
        done_df["API Number Normalized"] = done_df[API_COLUMN].apply(normalize_api)
        for _, row in done_df.iterrows():
            key = row["API Number Normalized"]
            if pd.notna(key) and pd.notna(row.get("Year Drilled Status")):
                cache[key] = (row.get("Year Drilled"), row.get("Year Drilled Status"))
    return cache


def scrape_years(df, cache, output_csv=OUTPUT_CSV, checkpoint_every=50):
    """
    Scrape the year drilled for all wells not already present in the cache.

    Iterates over every unique normalized API number in `df` that is missing
    from `cache`, calls fetch_year_drilled for each one, and updates the cache.
    Periodically saves a checkpoint to disk so progress is not lost if the
    scrape is interrupted. Prints a live progress bar with running ok /
    not_found / error counts.

    Args:
        df (pandas.DataFrame): Wells DataFrame with an "API Number Normalized" column,
                               as returned by load_wells.
        cache (dict): Existing cache of already-scraped results, as returned
                      by load_cache. Updated in place during the run.
        output_csv (str): Path to the output CSV file where results are saved.
                          Defaults to OUTPUT_CSV.
        checkpoint_every (int): Number of wells to scrape between intermediate
                                 saves to disk. Defaults to 50.

    Returns:
        dict: The updated cache containing results for all scraped wells.
    """
    unique_apis = [a for a in df["API Number Normalized"].dropna().unique() if a not in cache]
    total = len(unique_apis)
    print(f"APIs left to fetch: {total}")

    ok = sum(1 for v in cache.values() if v[1] == "ok")
    not_found = sum(1 for v in cache.values() if v[1] == "year_not_found")
    errors = sum(1 for v in cache.values() if v[1] == "error")

    bar = tqdm(unique_apis, desc="Scraping wells", unit="well", dynamic_ncols=True, file=sys.stdout)
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
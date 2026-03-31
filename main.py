"""
Main entry point for the geothermal wells data pipeline.

This module orchestrates the full pipeline:
  1. Load the raw well data from CSV.
  2. Normalize APINumber values to a consistent 8-digit format.
  3. Clean the dataset by removing empty/unusable columns.
  4. Scrape the year each well was drilled from the California GeoSteam website.

Output is written to Data/Processed/geothermal_wells_with_dates.csv.
"""

from scripts.load_data import load_raw_data
from scripts.data_cleaning import clean_data
from scripts.web_scraping import normalize_api, load_cache, scrape_years, INPUT_CSV, OUTPUT_CSV, API_COLUMN


def main():
    """
    Run the end-to-end geothermal wells data pipeline.

    Steps:
        1. Load the raw wells CSV from Data/Raw/.
        2. Normalize APINumber values to a consistent 8-digit format,
           storing the result in a new "api_norm" column.
        3. Clean the dataset by dropping columns that are entirely empty
           or zero-filled. Drop rows that are missing coordinates.
        4. Load any previously scraped results from the output CSV cache,
           then scrape the year-drilled for all remaining wells.

    Output is written to Data/Processed/geothermal_wells_with_dates.csv.
    """
    # Step 1: Load raw data
    print("Step 1: Loading raw data...")
    df = load_raw_data(INPUT_CSV)
    print(f"  Loaded {len(df)} rows from {INPUT_CSV}")

    # Step 2: Normalize API numbers
    print("\nStep 2: Normalizing API numbers...")
    df["api_norm"] = df[API_COLUMN].apply(normalize_api)
    print(f"  {df['api_norm'].notna().sum()} valid API numbers found")

    # Step 3: Clean the data
    print("\nStep 3: Cleaning data...")
    df = clean_data(df)

    '''
    # Step 4: Scrape year drilled from the GeoSteam website
    print("\nStep 4: Scraping year drilled from website...")
    cache = load_cache(OUTPUT_CSV)
    print(f"  {len(cache)} wells already cached")
    scrape_years(df, cache, OUTPUT_CSV)

    print("\nPipeline complete.")
    
    '''

if __name__ == "__main__":
    main()

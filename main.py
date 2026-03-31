# Main pipeline: loads, enriches, and cleans geothermal well data

from scripts.load_data import load_raw_data
from scripts.web_scraping import load_wells, load_cache, scrape_years, INPUT_CSV, OUTPUT_CSV


def main():
    # Step 1: Load raw data
    print("Step 1: Loading raw data...")
    raw_df = load_raw_data(INPUT_CSV)
    print(f"  Loaded {len(raw_df)} rows from {INPUT_CSV}")

    '''
    # Step 2: Normalize API numbers
    print("\nStep 2: Normalizing API numbers...")
    df = load_wells(INPUT_CSV)
    print(f"  {df['api_norm'].notna().sum()} valid API numbers found")

    # Step 3: Load scrape cache and scrape missing years
    print("\nStep 3: Scraping year drilled from website...")
    cache = load_cache(OUTPUT_CSV)
    print(f"  {len(cache)} wells already cached")
    cache = scrape_years(df, cache, OUTPUT_CSV)

    #TODO Finish with cleaning and enriching the data
    # Step 4: Data cleaning (placeholder — add steps as data_cleaning.py is built out)
    print("\nStep 4: Data cleaning...")
    print("  (No cleaning steps defined yet)")

    print("\nPipeline complete.")
    
    '''


if __name__ == "__main__":
    main()

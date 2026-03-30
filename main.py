# Main Code File to run the data inspection and cleaning scripts we created

from scripts.web_scraping import load_wells, load_cache, scrape_years, save_output, OUTPUT_CSV

df    = load_wells()
cache = load_cache()
cache = scrape_years(df, cache)
# File that inspects the data to get insights and what bits of data we have to work with.
import pandas as pd

df = pd.read_csv("Data/Raw/geothermal_wells_ca.csv")

# Inspect the size and shape of the data
print("Dataframe shape (Rows, Columns):", df.shape)

# See all column names
print("\nColumn names:", df.columns.tolist())

# Print the number of values in each column
print("\nNumber of values in each column:")
for col, count in df.count().items():
    print(f"  '{col}': {count} values")

# See how many values are MISSING per column
print("Number of NULL values in each column:")
print(df.isnull().sum())

# Print how many unique values are in each column
print("\nNumber of unique values in each column:")
for col, count in df.nunique().items():
    print(f"  '{col}': {count} unique values")
    
#Print unique values in specific columns
columns_to_inspect = ['WellStatus', 'WellType', 'WellSymbol', 'GeoDistrict', 'District','Confidential','FieldName','BaseMeridian','DatumCode','WellStatusDescription']
for col, count in df.nunique().items():
    if col in columns_to_inspect:
        print(f"\nUnique values in '{col}':")
        print(df[col].unique())
        

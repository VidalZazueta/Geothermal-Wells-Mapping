# File that inspects the data to get insights and what bits of data we have to work with.
import pandas as pd

df = pd.read_csv("geothermal_wells_ca.csv")

# See all column names
print("Columns:", df.columns.tolist())

# Print the number of values in each column
for col, count in df.count().items():
    print(f"  '{col}': {count} values")

'''
columns_to_inspect = []

for col in columns_to_inspect:
    unique_vals = df[col].dropna().unique().tolist()
    print(f"\nColumn: {col}")
    print(f"  Unique values ({len(unique_vals)}): {unique_vals}")
'''
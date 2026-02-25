import pandas as pd

# Read the CSV file
df = pd.read_csv('ClimatePolicyDatabase-v2024_clean.csv')

# Count rows with no content in the 'reference' column
# This checks for NaN, None, and empty strings
empty_reference = df['reference'].isna() | (df['reference'] == '')
count_empty = empty_reference.sum()

# Count rows with content
count_with_content = (~empty_reference).sum()

# Total rows
total_rows = len(df)

print(f"Total rows: {total_rows}")
print(f"Rows with NO content in 'reference' column: {count_empty}")
print(f"Rows WITH content in 'reference' column: {count_with_content}")
print(f"Percentage empty: {(count_empty/total_rows*100):.2f}%")
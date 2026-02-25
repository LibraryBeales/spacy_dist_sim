import pandas as pd

# Read the CSV file
df = pd.read_csv('language_report.csv')

# Get all unique language codes
unique_languages = df['language'].unique()
print("Unique language codes found:")
print(sorted(unique_languages))
print(f"\nTotal unique languages: {len(unique_languages)}")

# Count occurrences of each language
language_counts = df['language'].value_counts()
print("\nLanguage counts:")
print(language_counts)
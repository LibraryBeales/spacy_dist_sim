import pandas as pd

# Complete language mapping for your dataset
language_mapping = {
    'en': 'English',
    'es': 'Spanish',
    'de': 'German',
    'pt': 'Portuguese',
    'fr': 'French',
    'ca': 'Catalan',
    'it': 'Italian',
    'TOO_SHORT': 'Too Short to Detect',
    'da': 'Danish',
    'ru': 'Russian',
    'id': 'Indonesian',
    'sv': 'Swedish',
    'bn': 'Bengali',
    'tr': 'Turkish',
    'zh-cn': 'Chinese (Simplified)',
    'no': 'Norwegian',
    'nl': 'Dutch',
    'uk': 'Ukrainian',
    'ko': 'Korean',
    'ja': 'Japanese',
    'th': 'Thai',
    'vi': 'Vietnamese',
    'fi': 'Finnish',
    'sl': 'Slovenian',
    'sk': 'Slovak',
    'lv': 'Latvian',
    'et': 'Estonian',
    'af': 'Afrikaans',
    'ro': 'Romanian',
    'pl': 'Polish'
}

# Read CSV
df = pd.read_csv('language_report.csv')

# Map language codes to full names
df['language_full'] = df['language'].map(language_mapping)

# Get counts with full names
language_counts = df['language_full'].value_counts()

print(language_counts)

# Create summary DataFrame
language_summary = pd.DataFrame({
    'Language': language_counts.index,
    'Count': language_counts.values,
    'Percentage': (language_counts.values / len(df) * 100).round(2)
})

print("\n" + "="*50)
print(language_summary.to_string(index=False))
print("="*50)
print(f"\nTotal files: {len(df)}")

# Save to CSV
language_summary.to_csv('language_summary.csv', index=False)
print("\nSummary saved to 'language_summary.csv'")
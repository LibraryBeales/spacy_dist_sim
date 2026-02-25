"""
URL Reference Analyzer
Analyzes URL references in policy database CSV files and generates:
1. Total count of URLs across all reference columns
2. Statistics by URL type (pdf, html, etc.)
3. Analysis of empty/missing references
4. Error analysis for failed URL retrievals
"""

import pandas as pd
import argparse
from pathlib import Path
from urllib.parse import urlparse
import re
from collections import Counter
import matplotlib.pyplot as plt


def load_data(csv_path):
    """
    Load CSV data from file.
    
    Args:
        csv_path (str): Path to the input CSV file
        
    Returns:
        pd.DataFrame: Loaded dataframe
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"✓ Loaded {len(df)} records from {csv_path}")
        return df
    except FileNotFoundError:
        print(f"✗ Error: File '{csv_path}' not found")
        return None
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return None


def extract_url_extension(url):
    """
    Extract file extension from URL.
    
    Args:
        url (str): URL string
        
    Returns:
        str: File extension (lowercase) or 'html' if no extension
    """
    if pd.isna(url) or not isinstance(url, str):
        return None
    
    # Parse URL
    parsed = urlparse(url.strip())
    path = parsed.path.lower()
    
    # Check for common extensions
    extensions = ['.pdf', '.html', '.htm', '.doc', '.docx', '.xml', '.json', '.txt', '.csv']
    
    for ext in extensions:
        if path.endswith(ext):
            return ext.replace('.', '')
    
    # Check if extension exists in path (even with query params)
    match = re.search(r'\.([a-z0-9]{2,5})(?:\?|$)', path)
    if match:
        return match.group(1)
    
    # Default to html for URLs without clear extension
    if url.startswith('http'):
        return 'html'
    
    return 'unknown'


def is_valid_url(url):
    """
    Check if string is a valid URL.
    
    Args:
        url (str): String to check
        
    Returns:
        bool: True if valid URL
    """
    if pd.isna(url) or not isinstance(url, str):
        return False
    
    url = url.strip()
    
    # Basic URL pattern check
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return bool(url_pattern.match(url))


def analyze_references(df, reference_columns=None):
    """
    Analyze URL references in the dataframe.
    
    Args:
        df (pd.DataFrame): Input dataframe
        reference_columns (list): List of reference column names
        
    Returns:
        dict: Analysis results
    """
    if reference_columns is None:
        reference_columns = ['reference', 'reference2', 'reference3', 'reference4']
    
    # Verify columns exist
    existing_columns = [col for col in reference_columns if col in df.columns]
    missing_columns = [col for col in reference_columns if col not in df.columns]
    
    if missing_columns:
        print(f"⚠ Warning: Missing columns: {missing_columns}")
    
    if not existing_columns:
        print("✗ Error: No reference columns found")
        return None
    
    results = {
        'total_records': len(df),
        'reference_columns': existing_columns,
        'urls_by_column': {},
        'url_types': Counter(),
        'total_urls': 0,
        'total_empty': 0,
        'records_with_urls': 0,
        'records_without_urls': 0,
        'error_analysis': {},
        'url_details': []
    }
    
    # Track records with at least one URL
    records_with_urls = set()
    
    # Analyze each reference column
    for col in existing_columns:
        col_urls = 0
        col_empty = 0
        col_types = Counter()
        
        for idx, value in df[col].items():
            if is_valid_url(value):
                col_urls += 1
                url_type = extract_url_extension(value)
                col_types[url_type] += 1
                results['url_types'][url_type] += 1
                results['url_details'].append({
                    'policy_id': df.loc[idx, 'policy_id'] if 'policy_id' in df.columns else idx,
                    'column': col,
                    'url': value,
                    'type': url_type
                })
                records_with_urls.add(idx)
            else:
                col_empty += 1
        
        results['urls_by_column'][col] = {
            'count': col_urls,
            'empty': col_empty,
            'types': dict(col_types)
        }
        
        results['total_urls'] += col_urls
        results['total_empty'] += col_empty
    
    results['records_with_urls'] = len(records_with_urls)
    results['records_without_urls'] = len(df) - len(records_with_urls)
    
    # Analyze error columns if they exist
    error_columns = [f'{col.replace("reference", "ref")}_error' 
                     for col in existing_columns]
    
    for error_col in error_columns:
        if error_col in df.columns:
            error_counts = df[error_col].value_counts()
            if len(error_counts) > 0:
                results['error_analysis'][error_col] = error_counts.to_dict()
    
    return results


def print_analysis_report(results):
    """
    Print formatted analysis report.
    
    Args:
        results (dict): Analysis results
    """
    print("\n" + "=" * 70)
    print("  URL REFERENCE ANALYSIS REPORT")
    print("=" * 70)
    
    print(f"\n📊 OVERALL STATISTICS")
    print("-" * 70)
    print(f"  Total records:                {results['total_records']:,}")
    print(f"  Total URLs found:             {results['total_urls']:,}")
    print(f"  Total empty references:       {results['total_empty']:,}")
    print(f"  Records with at least 1 URL:  {results['records_with_urls']:,} ({results['records_with_urls']/results['total_records']*100:.1f}%)")
    print(f"  Records with no URLs:         {results['records_without_urls']:,} ({results['records_without_urls']/results['total_records']*100:.1f}%)")
    
    print(f"\n📑 URLS BY COLUMN")
    print("-" * 70)
    for col, stats in results['urls_by_column'].items():
        print(f"\n  {col}:")
        print(f"    URLs found:  {stats['count']:,}")
        print(f"    Empty:       {stats['empty']:,}")
        if stats['types']:
            print(f"    Types:       {dict(stats['types'])}")
    
    print(f"\n📄 URLS BY FILE TYPE")
    print("-" * 70)
    sorted_types = sorted(results['url_types'].items(), key=lambda x: x[1], reverse=True)
    
    max_count = max(results['url_types'].values()) if results['url_types'] else 1
    
    for url_type, count in sorted_types:
        percentage = (count / results['total_urls'] * 100) if results['total_urls'] > 0 else 0
        bar_length = int((count / max_count) * 40)
        bar = '█' * bar_length
        print(f"  {url_type:>10} │ {bar} {count:>5,} ({percentage:>5.1f}%)")
    
    # Error analysis
    if results['error_analysis']:
        print(f"\n⚠ ERROR ANALYSIS")
        print("-" * 70)
        for error_col, error_counts in results['error_analysis'].items():
            print(f"\n  {error_col}:")
            for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                if pd.notna(error_type):
                    print(f"    {count:>4,} × {error_type}")


def create_visualizations(results, output_dir='.'):
    """
    Create visualization charts for URL analysis.
    
    Args:
        results (dict): Analysis results
        output_dir (str): Output directory for charts
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # 1. URL Type Distribution Pie Chart
    if results['url_types']:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Pie chart
        types = list(results['url_types'].keys())
        counts = list(results['url_types'].values())
        colors = plt.cm.Set3(range(len(types)))
        
        ax1.pie(counts, labels=types, autopct='%1.1f%%', startangle=90, colors=colors)
        ax1.set_title('URL Distribution by File Type', fontsize=14, fontweight='bold', pad=20)
        
        # Bar chart
        ax2.barh(types, counts, color=colors, edgecolor='black', alpha=0.7)
        ax2.set_xlabel('Number of URLs', fontsize=11, fontweight='bold')
        ax2.set_title('URL Count by File Type', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')
        
        # Add count labels on bars
        for i, (type_name, count) in enumerate(zip(types, counts)):
            ax2.text(count, i, f' {count:,}', va='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(output_path / 'url_type_distribution.png', dpi=300, bbox_inches='tight')
        print(f"\n✓ Saved URL type distribution chart to '{output_path / 'url_type_distribution.png'}'")
        plt.close()
    
    # 2. URLs by Column Chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    columns = list(results['urls_by_column'].keys())
    url_counts = [results['urls_by_column'][col]['count'] for col in columns]
    empty_counts = [results['urls_by_column'][col]['empty'] for col in columns]
    
    x = range(len(columns))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], url_counts, width, label='URLs Found', 
                    color='steelblue', edgecolor='black', alpha=0.7)
    bars2 = ax.bar([i + width/2 for i in x], empty_counts, width, label='Empty', 
                    color='lightcoral', edgecolor='black', alpha=0.7)
    
    ax.set_xlabel('Reference Column', fontsize=11, fontweight='bold')
    ax.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax.set_title('URL Distribution Across Reference Columns', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(columns, rotation=15, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path / 'url_by_column.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved URL by column chart to '{output_path / 'url_by_column.png'}'")
    plt.close()
    
    # 3. Records with/without URLs
    fig, ax = plt.subplots(figsize=(8, 8))
    
    sizes = [results['records_with_urls'], results['records_without_urls']]
    labels = [f"With URLs\n({results['records_with_urls']:,})", 
              f"Without URLs\n({results['records_without_urls']:,})"]
    colors = ['#66b3ff', '#ff9999']
    explode = (0.05, 0)
    
    ax.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
           startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
    ax.set_title('Records with URL References', fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig(output_path / 'records_with_urls.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved records with URLs chart to '{output_path / 'records_with_urls.png'}'")
    plt.close()


def save_url_details(results, output_path='url_details.csv'):
    """
    Save detailed URL information to CSV.
    
    Args:
        results (dict): Analysis results
        output_path (str): Output CSV file path
    """
    if results['url_details']:
        df_details = pd.DataFrame(results['url_details'])
        df_details.to_csv(output_path, index=False)
        print(f"\n✓ Saved detailed URL list to '{output_path}'")


def save_summary_report(results, output_path='url_analysis_summary.txt'):
    """
    Save text summary report.
    
    Args:
        results (dict): Analysis results
        output_path (str): Output text file path
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("  URL REFERENCE ANALYSIS REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 70 + "\n")
        f.write(f"Total records:                {results['total_records']:,}\n")
        f.write(f"Total URLs found:             {results['total_urls']:,}\n")
        f.write(f"Total empty references:       {results['total_empty']:,}\n")
        f.write(f"Records with at least 1 URL:  {results['records_with_urls']:,} ({results['records_with_urls']/results['total_records']*100:.1f}%)\n")
        f.write(f"Records with no URLs:         {results['records_without_urls']:,} ({results['records_without_urls']/results['total_records']*100:.1f}%)\n\n")
        
        f.write("URLS BY COLUMN\n")
        f.write("-" * 70 + "\n")
        for col, stats in results['urls_by_column'].items():
            f.write(f"\n{col}:\n")
            f.write(f"  URLs found:  {stats['count']:,}\n")
            f.write(f"  Empty:       {stats['empty']:,}\n")
            if stats['types']:
                f.write(f"  Types:       {dict(stats['types'])}\n")
        
        f.write("\nURLs BY FILE TYPE\n")
        f.write("-" * 70 + "\n")
        sorted_types = sorted(results['url_types'].items(), key=lambda x: x[1], reverse=True)
        for url_type, count in sorted_types:
            percentage = (count / results['total_urls'] * 100) if results['total_urls'] > 0 else 0
            f.write(f"{url_type:>10} : {count:>5,} ({percentage:>5.1f}%)\n")
    
    print(f"✓ Saved summary report to '{output_path}'")


def main():
    """Main execution function."""
    
    parser = argparse.ArgumentParser(
        description='Analyze URL references in policy database CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_urls.py policy_data.csv
  python analyze_urls.py policy_data.csv --output-dir results
  python analyze_urls.py policy_data.csv --no-plots
  python analyze_urls.py policy_data.csv --columns reference reference2 reference3
        """
    )
    
    parser.add_argument('input_csv', help='Path to input CSV file')
    parser.add_argument('--columns', nargs='+', 
                        default=['reference', 'reference2', 'reference3', 'reference4'],
                        help='Reference column names (default: reference reference2 reference3 reference4)')
    parser.add_argument('--output-dir', default='.',
                        help='Output directory for charts and reports (default: current directory)')
    parser.add_argument('--no-plots', action='store_true',
                        help='Skip generating visualization plots')
    parser.add_argument('--save-details', action='store_true',
                        help='Save detailed URL list to CSV')
    parser.add_argument('--save-summary', action='store_true',
                        help='Save text summary report')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("  URL REFERENCE ANALYZER")
    print("=" * 70)
    
    # Load data
    df = load_data(args.input_csv)
    if df is None:
        return
    
    # Analyze references
    print(f"\n🔍 Analyzing reference columns: {args.columns}")
    results = analyze_references(df, reference_columns=args.columns)
    
    if results is None:
        return
    
    # Print report
    print_analysis_report(results)
    
    # Create visualizations
    if not args.no_plots:
        print(f"\n📊 Generating visualizations...")
        create_visualizations(results, output_dir=args.output_dir)
    
    # Save detailed URL list
    if args.save_details:
        output_path = Path(args.output_dir) / 'url_details.csv'
        save_url_details(results, output_path=str(output_path))
    
    # Save summary report
    if args.save_summary:
        output_path = Path(args.output_dir) / 'url_analysis_summary.txt'
        save_summary_report(results, output_path=str(output_path))
    
    print("\n" + "=" * 70)
    print("✓ Analysis complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
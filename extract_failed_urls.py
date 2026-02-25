"""
Extract Failed URLs from Scraping Results
Creates detailed reports of all URLs that failed during scraping,
categorized by error type (404, 403, timeout, etc.)
"""

import pandas as pd
import argparse
from pathlib import Path
from collections import defaultdict
import re


def load_policy_data(csv_path):
    """Load policy CSV data."""
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"✓ Loaded {len(df)} policy records from {csv_path}")
        return df
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return None


def extract_failed_urls(df, reference_columns):
    """
    Extract all URLs that had scraping errors.
    
    Args:
        df (pd.DataFrame): Input dataframe
        reference_columns (list): List of reference column names
        
    Returns:
        list: List of failed URL records
    """
    failed_urls = []
    
    for idx, row in df.iterrows():
        policy_id = row['policy_id']
        
        for i, col in enumerate(reference_columns, start=1):
            if col not in df.columns:
                continue
            
            url = row[col]
            error_col = f'ref{i}_error'
            
            # Check if there's an error
            if error_col in df.columns:
                error = row[error_col]
                
                if pd.notna(error) and str(error).strip():
                    # We have an error for this URL
                    failed_urls.append({
                        'policy_id': policy_id,
                        'reference_column': col,
                        'ref_number': i,
                        'url': url if pd.notna(url) else 'N/A',
                        'error': str(error).strip(),
                        'country': row.get('country', 'N/A'),
                        'policy_title': row.get('policy_title', 'N/A')
                    })
    
    return failed_urls


def categorize_error(error_msg):
    """
    Categorize error message into error type.
    
    Args:
        error_msg (str): Error message
        
    Returns:
        str: Error category
    """
    error_lower = error_msg.lower()
    
    # Check for specific error patterns
    if '404' in error_lower or 'not found' in error_lower:
        return '404_Not_Found'
    elif '403' in error_lower or 'forbidden' in error_lower:
        return '403_Forbidden'
    elif '401' in error_lower or 'unauthorized' in error_lower:
        return '401_Unauthorized'
    elif '500' in error_lower or 'internal server error' in error_lower:
        return '500_Server_Error'
    elif '502' in error_lower or 'bad gateway' in error_lower:
        return '502_Bad_Gateway'
    elif '503' in error_lower or 'service unavailable' in error_lower:
        return '503_Service_Unavailable'
    elif 'timeout' in error_lower or 'timed out' in error_lower:
        return 'Timeout'
    elif 'connection' in error_lower and 'refused' in error_lower:
        return 'Connection_Refused'
    elif 'connection' in error_lower:
        return 'Connection_Error'
    elif 'ssl' in error_lower or 'certificate' in error_lower:
        return 'SSL_Certificate_Error'
    elif 'redirect' in error_lower and 'too many' in error_lower:
        return 'Too_Many_Redirects'
    elif 'dns' in error_lower or 'name resolution' in error_lower:
        return 'DNS_Error'
    elif 'max retries' in error_lower:
        return 'Max_Retries_Exceeded'
    elif 'read timed out' in error_lower:
        return 'Read_Timeout'
    else:
        return 'Other_Error'


def analyze_failed_urls(failed_urls):
    """
    Analyze failed URLs and categorize by error type.
    
    Args:
        failed_urls (list): List of failed URL records
        
    Returns:
        dict: Analysis results
    """
    # Categorize by error type
    errors_by_category = defaultdict(list)
    
    for record in failed_urls:
        category = categorize_error(record['error'])
        record['error_category'] = category
        errors_by_category[category].append(record)
    
    # Count by category
    category_counts = {cat: len(records) for cat, records in errors_by_category.items()}
    
    # Sort by count
    sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
    
    return {
        'total_errors': len(failed_urls),
        'errors_by_category': dict(errors_by_category),
        'category_counts': dict(sorted_categories),
        'sorted_categories': sorted_categories
    }


def print_error_summary(analysis):
    """Print summary of errors."""
    print("\n" + "=" * 85)
    print("  FAILED URLs SUMMARY")
    print("=" * 85)
    
    print(f"\n📊 Total Failed URLs: {analysis['total_errors']:,}")
    print("\n📋 Breakdown by Error Type:")
    print("-" * 85)
    
    for category, count in analysis['sorted_categories']:
        percentage = (count / analysis['total_errors'] * 100) if analysis['total_errors'] > 0 else 0
        bar_length = int((count / analysis['total_errors']) * 50)
        bar = '█' * bar_length
        
        print(f"  {category:.<30} {bar} {count:>5,} ({percentage:>5.1f}%)")


def save_failed_urls_report(failed_urls, analysis, output_dir='.'):
    """
    Save detailed reports of failed URLs.
    
    Args:
        failed_urls (list): List of failed URL records
        analysis (dict): Analysis results
        output_dir (str): Output directory
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # 1. Save all failed URLs to single CSV
    df_all_failed = pd.DataFrame(failed_urls)
    all_failed_path = output_path / 'all_failed_urls.csv'
    df_all_failed.to_csv(all_failed_path, index=False)
    print(f"\n✓ Saved all failed URLs to '{all_failed_path}'")
    print(f"  Total records: {len(df_all_failed):,}")
    
    # 2. Save separate CSV for each error category
    for category, records in analysis['errors_by_category'].items():
        df_category = pd.DataFrame(records)
        category_path = output_path / f'failed_urls_{category}.csv'
        df_category.to_csv(category_path, index=False)
        print(f"✓ Saved {len(records):,} {category} errors to '{category_path}'")
    
    # 3. Save summary report
    summary_path = output_path / 'failed_urls_summary.txt'
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("FAILED URLs SUMMARY REPORT\n")
        f.write("=" * 85 + "\n\n")
        f.write(f"Total Failed URLs: {analysis['total_errors']:,}\n\n")
        f.write("Breakdown by Error Type:\n")
        f.write("-" * 85 + "\n")
        
        for category, count in analysis['sorted_categories']:
            percentage = (count / analysis['total_errors'] * 100) if analysis['total_errors'] > 0 else 0
            f.write(f"{category:.<40} {count:>6,} ({percentage:>5.1f}%)\n")
        
        f.write("\n" + "=" * 85 + "\n\n")
        f.write("ERROR CATEGORY DESCRIPTIONS:\n")
        f.write("-" * 85 + "\n")
        f.write("404_Not_Found:           Page doesn't exist or was removed\n")
        f.write("403_Forbidden:           Access denied (IP blocked, authentication required)\n")
        f.write("401_Unauthorized:        Authentication required\n")
        f.write("500_Server_Error:        Server-side error\n")
        f.write("Timeout:                 Server didn't respond in time\n")
        f.write("Connection_Error:        Network connection issues\n")
        f.write("Connection_Refused:      Server refused connection\n")
        f.write("SSL_Certificate_Error:   SSL/TLS certificate problems\n")
        f.write("DNS_Error:               Domain name resolution failed\n")
        f.write("Read_Timeout:            Server stopped responding during download\n")
        f.write("Max_Retries_Exceeded:    Too many failed retry attempts\n")
        f.write("Other_Error:             Miscellaneous errors\n")
    
    print(f"✓ Saved summary report to '{summary_path}'")
    
    # 4. Save URLs only (for easy retry)
    urls_only_path = output_path / 'failed_urls_list.txt'
    with open(urls_only_path, 'w', encoding='utf-8') as f:
        for record in failed_urls:
            if record['url'] != 'N/A':
                f.write(f"{record['url']}\n")
    print(f"✓ Saved URL list (for retry) to '{urls_only_path}'")


def create_error_visualization(analysis, output_dir='.'):
    """Create visualization of error distribution."""
    try:
        import matplotlib.pyplot as plt
        
        output_path = Path(output_dir)
        
        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
        
        # 1. Pie chart
        categories = [cat for cat, _ in analysis['sorted_categories']]
        counts = [count for _, count in analysis['sorted_categories']]
        colors = plt.cm.Set3(range(len(categories)))
        
        wedges, texts, autotexts = ax1.pie(counts, labels=categories, autopct='%1.1f%%',
                                            startangle=90, colors=colors)
        for text in texts:
            text.set_fontsize(9)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(9)
        
        ax1.set_title(f'Failed URLs by Error Type\n(Total: {analysis["total_errors"]:,})',
                      fontsize=14, fontweight='bold', pad=20)
        
        # 2. Horizontal bar chart
        y_pos = range(len(categories))
        bars = ax2.barh(y_pos, counts, color=colors, edgecolor='black', alpha=0.7)
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(categories, fontsize=9)
        ax2.set_xlabel('Number of Failed URLs', fontsize=11, fontweight='bold')
        ax2.set_title('Error Distribution', fontsize=14, fontweight='bold', pad=20)
        ax2.grid(axis='x', alpha=0.3, linestyle='--')
        
        # Add count labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            width = bar.get_width()
            ax2.text(width, i, f' {count:,}', va='center', fontsize=9, fontweight='bold')
        
        plt.tight_layout()
        chart_path = output_path / 'failed_urls_chart.png'
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        print(f"✓ Saved error distribution chart to '{chart_path}'")
        plt.close()
        
    except ImportError:
        print("⚠ matplotlib not available, skipping visualization")


def generate_retry_script(failed_urls, analysis, output_dir='.'):
    """
    Generate a Python script to retry failed URLs.
    
    Args:
        failed_urls (list): List of failed URL records
        analysis (dict): Analysis results
        output_dir (str): Output directory
    """
    output_path = Path(output_dir)
    
    # Filter out certain error types that shouldn't be retried
    no_retry_categories = ['404_Not_Found', '403_Forbidden', '401_Unauthorized']
    
    retryable = [
        record for record in failed_urls 
        if record['error_category'] not in no_retry_categories
    ]
    
    script_path = output_path / 'retry_failed_urls.py'
    
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write('''"""
Retry Failed URLs Script
Auto-generated script to retry URLs that failed during scraping.
Only includes errors that are likely temporary (timeouts, connection errors, etc.)
"""

import requests
import time
from pathlib import Path

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; policy-scraper/1.0)"
}

TIMEOUT = 30
OUTPUT_DIR = "scraped_policy_docs2_retry"
Path(OUTPUT_DIR).mkdir(exist_ok=True)

# URLs to retry
RETRY_URLS = [
''')
        
        for record in retryable:
            f.write(f'    {{\n')
            f.write(f'        "policy_id": "{record["policy_id"]}",\n')
            f.write(f'        "ref_num": {record["ref_number"]},\n')
            f.write(f'        "url": "{record["url"]}",\n')
            f.write(f'        "original_error": "{record["error"][:50]}..."\n')
            f.write(f'    }},\n')
        
        f.write(''']

def retry_url(record):
    """Retry downloading a single URL."""
    url = record["url"]
    policy_id = record["policy_id"]
    ref_num = record["ref_num"]
    
    try:
        print(f"Retrying: {policy_id}_ref{ref_num}")
        print(f"  URL: {url[:80]}...")
        
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Determine file type
        content_type = response.headers.get("content-type", "").lower()
        
        if "pdf" in content_type or url.lower().endswith(".pdf"):
            filename = f"{policy_id}_ref{ref_num}.pdf"
            mode = "wb"
            content = response.content
        else:
            filename = f"{policy_id}_ref{ref_num}.txt"
            mode = "w"
            content = response.text
        
        filepath = Path(OUTPUT_DIR) / filename
        
        if mode == "wb":
            with open(filepath, mode) as f:
                f.write(content)
        else:
            with open(filepath, mode, encoding="utf-8") as f:
                f.write(content)
        
        print(f"  ✓ Success! Saved to {filename}")
        return True
        
    except Exception as e:
        print(f"  ✗ Failed again: {e}")
        return False

# Main retry loop
print(f"Retrying {len(RETRY_URLS)} URLs...")
print("=" * 80)

success_count = 0
fail_count = 0

for i, record in enumerate(RETRY_URLS, 1):
    print(f"\\n[{i}/{len(RETRY_URLS)}]")
    
    if retry_url(record):
        success_count += 1
    else:
        fail_count += 1
    
    time.sleep(2)  # Be polite to servers

print("\\n" + "=" * 80)
print(f"Retry complete!")
print(f"  Success: {success_count}")
print(f"  Failed:  {fail_count}")
print(f"  Files saved to: {OUTPUT_DIR}")
''')
    
    print(f"✓ Generated retry script to '{script_path}'")
    print(f"  Includes {len(retryable):,} retryable URLs (excludes 404s, 403s, etc.)")
    print(f"  Run with: python {script_path}")


def main():
    """Main execution function."""
    
    parser = argparse.ArgumentParser(
        description='Extract and analyze failed URLs from scraping results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_failed_urls.py policies_with_downloads2.csv
  python extract_failed_urls.py policies_with_downloads2.csv --output-dir failed_urls_report
  python extract_failed_urls.py policies_with_downloads2.csv --generate-retry-script
        """
    )
    
    parser.add_argument('input_csv',
                        help='Path to CSV file with scraping results')
    parser.add_argument('--output-dir', default='failed_urls_report',
                        help='Output directory for reports (default: failed_urls_report)')
    parser.add_argument('--no-charts', action='store_true',
                        help='Skip generating charts')
    parser.add_argument('--generate-retry-script', action='store_true',
                        help='Generate a Python script to retry failed URLs')
    
    args = parser.parse_args()
    
    print("=" * 85)
    print("  FAILED URLs EXTRACTION TOOL")
    print("=" * 85)
    
    # Load data
    df = load_policy_data(args.input_csv)
    if df is None:
        return
    
    # Extract failed URLs
    reference_columns = ['reference', 'reference2', 'reference3', 'reference4']
    print(f"\n🔍 Extracting failed URLs from error columns...")
    failed_urls = extract_failed_urls(df, reference_columns)
    
    if not failed_urls:
        print("\n✓ Great news! No failed URLs found.")
        print("  All scraping attempts were successful!")
        return
    
    print(f"✓ Found {len(failed_urls):,} failed URLs")
    
    # Analyze errors
    print(f"\n📊 Analyzing error types...")
    analysis = analyze_failed_urls(failed_urls)
    
    # Print summary
    print_error_summary(analysis)
    
    # Save reports
    print(f"\n💾 Saving detailed reports to '{args.output_dir}'...")
    save_failed_urls_report(failed_urls, analysis, output_dir=args.output_dir)
    
    # Create visualization
    if not args.no_charts:
        print(f"\n📊 Generating visualizations...")
        create_error_visualization(analysis, output_dir=args.output_dir)
    
    # Generate retry script
    if args.generate_retry_script:
        print(f"\n🔄 Generating retry script...")
        generate_retry_script(failed_urls, analysis, output_dir=args.output_dir)
    
    print("\n" + "=" * 85)
    print("✓ Extraction complete!")
    print("=" * 85)
    
    # Print file locations
    print(f"\n📁 Output files saved to: {args.output_dir}/")
    print(f"  • all_failed_urls.csv - Complete list with all details")
    print(f"  • failed_urls_[category].csv - Separate files by error type")
    print(f"  • failed_urls_summary.txt - Text summary report")
    print(f"  • failed_urls_list.txt - Plain list of URLs (for easy retry)")
    if not args.no_charts:
        print(f"  • failed_urls_chart.png - Visual error distribution")
    if args.generate_retry_script:
        print(f"  • retry_failed_urls.py - Automated retry script")


if __name__ == "__main__":
    main()
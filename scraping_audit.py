"""
Policy Scraping Audit Tool
Analyzes scraping results to identify why fewer documents were saved than URLs attempted.
Works with policy CSV structure and naming convention: policy_id_ref#
"""

import os
import pandas as pd
import argparse
from pathlib import Path
from collections import Counter, defaultdict
import matplotlib.pyplot as plt
from urllib.parse import urlparse, parse_qs
import re


def load_policy_data(csv_path):
    """
    Load policy CSV data.
    
    Args:
        csv_path (str): Path to the input CSV file
        
    Returns:
        pd.DataFrame: Loaded dataframe
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"✓ Loaded {len(df)} policy records from {csv_path}")
        return df
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return None


def extract_all_urls(df, reference_columns):
    """
    Extract all URLs from reference columns with metadata.
    
    Args:
        df (pd.DataFrame): Input dataframe
        reference_columns (list): List of reference column names
        
    Returns:
        list: List of dicts with URL metadata
    """
    url_list = []
    
    for idx, row in df.iterrows():
        policy_id = row['policy_id']
        
        for i, col in enumerate(reference_columns, start=1):
            if col not in df.columns:
                continue
            
            url = row[col]
            
            # Check if URL exists and is valid
            if pd.notna(url) and isinstance(url, str) and url.strip():
                url_clean = url.strip()
                
                # Get corresponding error and type columns
                error_col = f'ref{i}_error'
                type_col = f'ref{i}_type'
                
                error = row.get(error_col, None)
                file_type = row.get(type_col, None)
                
                url_list.append({
                    'policy_id': policy_id,
                    'ref_num': i,
                    'url': url_clean,
                    'error': error if pd.notna(error) else None,
                    'file_type': file_type if pd.notna(file_type) else None,
                    'expected_filename': f"{policy_id}_ref{i}"
                })
    
    return url_list


def analyze_url_duplicates(url_list):
    """
    Analyze duplicate URLs.
    
    Args:
        url_list (list): List of URL metadata dicts
        
    Returns:
        dict: Duplicate analysis results
    """
    urls_only = [item['url'] for item in url_list]
    url_counts = Counter(urls_only)
    duplicates = {url: count for url, count in url_counts.items() if count > 1}
    
    # Find which policies share the same URL
    url_to_policies = defaultdict(list)
    for item in url_list:
        url_to_policies[item['url']].append(f"{item['policy_id']}_ref{item['ref_num']}")
    
    duplicate_details = {
        url: {
            'count': count,
            'policies': url_to_policies[url]
        }
        for url, count in duplicates.items()
    }
    
    total_urls = len(urls_only)
    unique_urls = len(url_counts)
    duplicate_count = sum(count - 1 for count in duplicates.values())
    
    return {
        'total_urls': total_urls,
        'unique_urls': unique_urls,
        'duplicate_urls': len(duplicates),
        'duplicate_instances': duplicate_count,
        'duplicates': duplicate_details
    }


def canonicalize_url(url):
    """
    Normalize URL to standard form (remove parameters, www, trailing slashes).
    
    Args:
        url (str): URL to canonicalize
        
    Returns:
        str: Canonicalized URL
    """
    parsed = urlparse(url.lower())
    scheme = parsed.scheme or 'https'
    netloc = parsed.netloc.replace('www.', '')
    path = parsed.path.rstrip('/') or '/'
    # Ignore query parameters and fragments
    return f"{scheme}://{netloc}{path}"


def analyze_canonical_duplicates(url_list):
    """
    Analyze URLs that are duplicates after canonicalization.
    
    Args:
        url_list (list): List of URL metadata dicts
        
    Returns:
        dict: Canonical duplicate analysis
    """
    canonical_map = defaultdict(list)
    
    for item in url_list:
        canonical = canonicalize_url(item['url'])
        canonical_map[canonical].append({
            'original_url': item['url'],
            'policy_ref': f"{item['policy_id']}_ref{item['ref_num']}"
        })
    
    canonical_duplicates = {
        canonical: variants 
        for canonical, variants in canonical_map.items() 
        if len(variants) > 1
    }
    
    canonical_duplicate_count = sum(
        len(variants) - 1 
        for variants in canonical_duplicates.values()
    )
    
    return {
        'unique_canonical_urls': len(canonical_map),
        'canonical_duplicates': len(canonical_duplicates),
        'canonical_duplicate_instances': canonical_duplicate_count,
        'examples': dict(list(canonical_duplicates.items())[:5])
    }


def analyze_scraping_errors(url_list):
    """
    Analyze scraping errors from URL list.
    
    Args:
        url_list (list): List of URL metadata dicts
        
    Returns:
        dict: Error analysis results
    """
    errors = [item for item in url_list if item['error'] is not None]
    
    error_types = Counter(item['error'] for item in errors)
    
    # Categorize errors
    error_categories = {
        '404_not_found': [],
        '403_forbidden': [],
        'timeout': [],
        'connection_error': [],
        'ssl_error': [],
        'other': []
    }
    
    for item in errors:
        error_msg = str(item['error']).lower()
        
        if '404' in error_msg or 'not found' in error_msg:
            error_categories['404_not_found'].append(item)
        elif '403' in error_msg or 'forbidden' in error_msg:
            error_categories['403_forbidden'].append(item)
        elif 'timeout' in error_msg or 'timed out' in error_msg:
            error_categories['timeout'].append(item)
        elif 'connection' in error_msg:
            error_categories['connection_error'].append(item)
        elif 'ssl' in error_msg or 'certificate' in error_msg:
            error_categories['ssl_error'].append(item)
        else:
            error_categories['other'].append(item)
    
    return {
        'total_errors': len(errors),
        'error_types': dict(error_types),
        'error_categories': {k: len(v) for k, v in error_categories.items() if v},
        'error_details': error_categories
    }


def analyze_saved_documents(doc_directory, url_list):
    """
    Analyze saved documents in directory and match with expected files.
    
    Args:
        doc_directory (str): Path to directory with saved documents
        url_list (list): List of URL metadata dicts
        
    Returns:
        dict: Document analysis results
    """
    doc_path = Path(doc_directory)
    
    if not doc_path.exists():
        return {
            'total_saved': 0,
            'error': f"Directory '{doc_directory}' not found"
        }
    
    # Get all saved files
    saved_files = {}
    for f in doc_path.iterdir():
        if f.is_file() and not f.name.startswith('.'):
            # Extract policy_id and ref number from filename
            match = re.match(r'(\d+)_ref(\d+)\.(txt|pdf)', f.name)
            if match:
                policy_id = match.group(1)
                ref_num = match.group(2)
                file_type = match.group(3)
                key = f"{policy_id}_ref{ref_num}"
                saved_files[key] = {
                    'filename': f.name,
                    'path': str(f),
                    'size': f.stat().st_size,
                    'type': file_type
                }
    
    # Match expected files with saved files
    expected_files = {item['expected_filename']: item for item in url_list}
    
    matched = []
    missing = []
    
    for expected_name, url_item in expected_files.items():
        if expected_name in saved_files:
            matched.append({
                'expected': expected_name,
                'saved': saved_files[expected_name],
                'url': url_item['url']
            })
        else:
            missing.append({
                'expected': expected_name,
                'url': url_item['url'],
                'error': url_item['error']
            })
    
    # Analyze file sizes
    file_sizes = [f['size'] for f in saved_files.values()]
    empty_files = sum(1 for size in file_sizes if size == 0)
    very_small_files = sum(1 for size in file_sizes if 0 < size < 100)
    
    # File type distribution
    file_types = Counter(f['type'] for f in saved_files.values())
    
    return {
        'total_saved': len(saved_files),
        'total_expected': len(expected_files),
        'matched': len(matched),
        'missing': len(missing),
        'empty_files': empty_files,
        'very_small_files': very_small_files,
        'avg_file_size': sum(file_sizes) / len(file_sizes) if file_sizes else 0,
        'min_file_size': min(file_sizes) if file_sizes else 0,
        'max_file_size': max(file_sizes) if file_sizes else 0,
        'file_types': dict(file_types),
        'saved_files': saved_files,
        'missing_details': missing[:10]  # First 10 missing files
    }


def generate_audit_report(url_list, duplicate_analysis, canonical_analysis, 
                         error_analysis, doc_analysis):
    """
    Generate comprehensive audit report.
    
    Args:
        url_list (list): List of URL metadata
        duplicate_analysis (dict): Duplicate analysis results
        canonical_analysis (dict): Canonical duplicate analysis
        error_analysis (dict): Error analysis results
        doc_analysis (dict): Document analysis results
        
    Returns:
        dict: Complete audit report
    """
    total_urls = len(url_list)
    saved_docs = doc_analysis.get('total_saved', 0)
    gap = total_urls - saved_docs
    
    # Account for various issues
    reasons = []
    accounted_for = 0
    
    # 1. Exact duplicates (only count duplicates beyond the first)
    if duplicate_analysis['duplicate_instances'] > 0:
        accounted_for += duplicate_analysis['duplicate_instances']
        reasons.append({
            'reason': 'Exact duplicate URLs (same URL listed multiple times)',
            'count': duplicate_analysis['duplicate_instances'],
            'percentage': (duplicate_analysis['duplicate_instances'] / gap * 100) if gap > 0 else 0,
            'details': f"{duplicate_analysis['duplicate_urls']} unique URLs appear multiple times"
        })
    
    # 2. Canonical duplicates (after accounting for exact duplicates)
    canonical_only = canonical_analysis['canonical_duplicate_instances'] - duplicate_analysis['duplicate_instances']
    if canonical_only > 0:
        accounted_for += canonical_only
        reasons.append({
            'reason': 'Canonical duplicates (same page, different URL parameters)',
            'count': canonical_only,
            'percentage': (canonical_only / gap * 100) if gap > 0 else 0,
            'details': f"{canonical_analysis['canonical_duplicates']} base URLs with parameter variants"
        })
    
    # 3. Scraping errors
    if error_analysis['total_errors'] > 0:
        accounted_for += error_analysis['total_errors']
        reasons.append({
            'reason': 'Scraping errors (404, 403, timeouts, connection issues)',
            'count': error_analysis['total_errors'],
            'percentage': (error_analysis['total_errors'] / gap * 100) if gap > 0 else 0,
            'details': error_analysis['error_categories']
        })
    
    # 4. Empty or very small files
    problematic_files = doc_analysis.get('empty_files', 0) + doc_analysis.get('very_small_files', 0)
    if problematic_files > 0:
        reasons.append({
            'reason': 'Empty or very small files (likely failed downloads)',
            'count': problematic_files,
            'percentage': (problematic_files / gap * 100) if gap > 0 else 0,
            'details': f"{doc_analysis.get('empty_files', 0)} empty, {doc_analysis.get('very_small_files', 0)} very small"
        })
    
    # 5. Unaccounted for
    unaccounted = gap - accounted_for
    if unaccounted > 0:
        reasons.append({
            'reason': 'Other issues (blocked IPs, CAPTCHAs, robots.txt, paywalls)',
            'count': unaccounted,
            'percentage': (unaccounted / gap * 100) if gap > 0 else 0,
            'details': 'May require manual investigation'
        })
    
    return {
        'total_urls': total_urls,
        'unique_urls': duplicate_analysis['unique_urls'],
        'saved_documents': saved_docs,
        'missing_documents': gap,
        'missing_percentage': (gap / total_urls * 100) if total_urls > 0 else 0,
        'reasons': reasons,
        'accounted_for': accounted_for,
        'unaccounted_for': max(0, unaccounted)
    }


def print_audit_report(report, duplicate_analysis, canonical_analysis, 
                      error_analysis, doc_analysis):
    """
    Print formatted audit report.
    """
    print("\n" + "=" * 85)
    print("  POLICY SCRAPING AUDIT REPORT")
    print("=" * 85)
    
    print(f"\n📊 SUMMARY")
    print("-" * 85)
    print(f"  Total URL references:        {report['total_urls']:>7,}")
    print(f"  Unique URLs:                 {report['unique_urls']:>7,}")
    print(f"  Documents saved:             {report['saved_documents']:>7,}")
    print(f"  Missing documents:           {report['missing_documents']:>7,} ({report['missing_percentage']:>5.1f}%)")
    
    if 'error' not in doc_analysis:
        print(f"\n  File statistics:")
        print(f"    Empty files:               {doc_analysis.get('empty_files', 0):>7,}")
        print(f"    Very small files (<100B):  {doc_analysis.get('very_small_files', 0):>7,}")
        print(f"    Average file size:         {doc_analysis.get('avg_file_size', 0):>7,.0f} bytes")
        print(f"    File types: {doc_analysis.get('file_types', {})}")
    
    print(f"\n🔍 WHY ARE {report['missing_documents']:,} DOCUMENTS MISSING?")
    print("=" * 85)
    
    for i, reason in enumerate(report['reasons'], 1):
        print(f"\n  {i}. {reason['reason']}")
        print(f"     Count: {reason['count']:,} ({reason['percentage']:.1f}% of missing)")
        if isinstance(reason['details'], dict):
            print(f"     Breakdown:")
            for key, value in reason['details'].items():
                if value > 0:
                    print(f"       • {key.replace('_', ' ').title()}: {value:,}")
        else:
            print(f"     Details: {reason['details']}")
    
    print(f"\n📈 DETAILED BREAKDOWN")
    print("=" * 85)
    
    # Duplicate details
    if duplicate_analysis['duplicate_urls'] > 0:
        print(f"\n  ⚠ Exact Duplicates:")
        print(f"    • {duplicate_analysis['duplicate_urls']:,} URLs appear multiple times")
        print(f"    • {duplicate_analysis['duplicate_instances']:,} duplicate instances (won't be saved)")
        print(f"\n    Top 5 most duplicated URLs:")
        
        top_dupes = sorted(
            duplicate_analysis['duplicates'].items(), 
            key=lambda x: x[1]['count'], 
            reverse=True
        )[:5]
        
        for url, info in top_dupes:
            print(f"      • {info['count']}× occurrences")
            print(f"        URL: {url[:75]}...")
            print(f"        Policies: {', '.join(info['policies'][:5])}")
            if len(info['policies']) > 5:
                print(f"... and {len(info['policies']) - 5} more")
            print()
    
    # Canonical duplicates
    if canonical_analysis['canonical_duplicates'] > 0:
        print(f"\n  ⚠ Canonical Duplicates (same content, different URLs):")
        print(f"    • {canonical_analysis['canonical_duplicates']:,} base URLs have variants")
        print(f"    • {canonical_analysis['canonical_duplicate_instances']:,} total variant instances")
        print(f"\n    Examples (showing URL variants that point to same content):")
        
        for i, (canonical, variants) in enumerate(list(canonical_analysis['examples'].items())[:3], 1):
            print(f"\n      Example {i} - Base URL: {canonical}")
            for variant in variants[:4]:
                print(f"        ↳ {variant['original_url']}")
                print(f"          (Policy: {variant['policy_ref']})")
            if len(variants) > 4:
                print(f"... and {len(variants) - 4} more variants")
    
    # Error details
    if error_analysis['total_errors'] > 0:
        print(f"\n  ❌ Scraping Errors:")
        print(f"    • Total errors: {error_analysis['total_errors']:,}")
        print(f"\n    Error breakdown:")
        
        for category, count in sorted(error_analysis['error_categories'].items(), 
                                      key=lambda x: x[1], reverse=True):
            print(f"      • {category.replace('_', ' ').title()}: {count:,}")
        
        print(f"\n    Most common error messages (top 5):")
        top_errors = sorted(error_analysis['error_types'].items(), 
                           key=lambda x: x[1], reverse=True)[:5]
        for error_msg, count in top_errors:
            print(f"      • {count:>4,}× {error_msg[:70]}...")
    
    # Missing files details
    if 'missing_details' in doc_analysis and doc_analysis['missing_details']:
        print(f"\n  📄 Sample of Missing Files:")
        for item in doc_analysis['missing_details'][:5]:
            print(f"    • {item['expected']}")
            print(f"      URL: {item['url'][:70]}...")
            if item['error']:
                print(f"      Error: {item['error'][:70]}...")


def create_visualizations(report, error_analysis, doc_analysis, output_dir='.'):
    """
    Create visualization charts for audit report.
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Figure 1: Missing documents breakdown
    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(2, 3, hspace=0.3, wspace=0.3)
    
    # 1. Pie chart - Why documents are missing
    ax1 = fig.add_subplot(gs[0, :2])
    reasons = [r['reason'][:50] + '...' if len(r['reason']) > 50 else r['reason'] 
               for r in report['reasons']]
    counts = [r['count'] for r in report['reasons']]
    colors = plt.cm.Set3(range(len(reasons)))
    
    wedges, texts, autotexts = ax1.pie(counts, labels=reasons, autopct='%1.1f%%', 
                                        startangle=90, colors=colors)
    for text in texts:
        text.set_fontsize(9)
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(10)
    
    ax1.set_title(f'Why {report["missing_documents"]:,} Documents Are Missing', 
                  fontsize=14, fontweight='bold', pad=20)
    
    # 2. Bar chart - Overall summary
    ax2 = fig.add_subplot(gs[0, 2])
    categories = ['Total\nURLs', 'Saved\nDocs', 'Missing']
    values = [report['total_urls'], report['saved_documents'], report['missing_documents']]
    colors_bar = ['steelblue', 'green', 'coral']
    
    bars = ax2.bar(categories, values, color=colors_bar, edgecolor='black', alpha=0.7)
    ax2.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax2.set_title('Scraping Results', fontsize=12, fontweight='bold', pad=15)
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}', ha='center', va='bottom', 
                fontsize=9, fontweight='bold')
    
    # 3. Error categories
    if error_analysis['error_categories']:
        ax3 = fig.add_subplot(gs[1, :])
        error_cats = list(error_analysis['error_categories'].keys())
        error_counts = list(error_analysis['error_categories'].values())
        
        bars = ax3.barh(error_cats, error_counts, color='salmon', edgecolor='black', alpha=0.7)
        ax3.set_xlabel('Number of Errors', fontsize=11, fontweight='bold')
        ax3.set_title('Error Categories Breakdown', fontsize=12, fontweight='bold', pad=15)
        ax3.grid(axis='x', alpha=0.3, linestyle='--')
        
        for i, (cat, count) in enumerate(zip(error_cats, error_counts)):
            ax3.text(count, i, f' {count:,}', va='center', fontsize=9, fontweight='bold')
    
    plt.savefig(output_path / 'scraping_audit.png', dpi=300, bbox_inches='tight')
    print(f"\n✓ Saved audit visualization to '{output_path / 'scraping_audit.png'}'")
    plt.close()
    
    # Figure 2: File type distribution (if we have saved docs)
    if 'file_types' in doc_analysis and doc_analysis['file_types']:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        file_types = list(doc_analysis['file_types'].keys())
        file_counts = list(doc_analysis['file_types'].values())
        
        bars = ax.bar(file_types, file_counts, color=['#ff6b6b', '#4ecdc4'], 
                     edgecolor='black', alpha=0.7)
        ax.set_ylabel('Number of Files', fontsize=11, fontweight='bold')
        ax.set_xlabel('File Type', fontsize=11, fontweight='bold')
        ax.set_title('Saved Documents by File Type', fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height):,}', ha='center', va='bottom', 
                   fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(output_path / 'file_types.png', dpi=300, bbox_inches='tight')
        print(f"✓ Saved file type distribution to '{output_path / 'file_types.png'}'")
        plt.close()


def save_detailed_reports(url_list, duplicate_analysis, error_analysis, 
                         doc_analysis, output_dir='.'):
    """
    Save detailed CSV reports.
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # 1. All URLs with status
    df_urls = pd.DataFrame(url_list)
    df_urls['saved'] = df_urls['expected_filename'].apply(
        lambda x: x in doc_analysis.get('saved_files', {})
    )
    df_urls.to_csv(output_path / 'url_analysis.csv', index=False)
    print(f"✓ Saved URL analysis to '{output_path / 'url_analysis.csv'}'")
    
    # 2. Duplicate URLs
    if duplicate_analysis['duplicates']:
        dup_records = []
        for url, info in duplicate_analysis['duplicates'].items():
            dup_records.append({
                'url': url,
                'occurrences': info['count'],
                'policies': ', '.join(info['policies'])
            })
        df_dups = pd.DataFrame(dup_records)
        df_dups = df_dups.sort_values('occurrences', ascending=False)
        df_dups.to_csv(output_path / 'duplicate_urls.csv', index=False)
        print(f"✓ Saved duplicate URLs to '{output_path / 'duplicate_urls.csv'}'")
    
    # 3. Failed URLs
    failed_urls = [item for item in url_list if item['error'] is not None]
    if failed_urls:
        df_failed = pd.DataFrame(failed_urls)
        df_failed.to_csv(output_path / 'failed_urls.csv', index=False)
        print(f"✓ Saved failed URLs to '{output_path / 'failed_urls.csv'}'")


def main():
    """Main execution function."""
    
    parser = argparse.ArgumentParser(
        description='Audit policy scraping results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraping_audit.py policies_with_downloads2.csv --doc-dir scraped_policy_docs2
  python scraping_audit.py policies_with_downloads2.csv --doc-dir scraped_policy_docs2 --output-dir audit_results
  python scraping_audit.py policies_with_downloads2.csv --doc-dir scraped_policy_docs2 --no-plots
        """
    )
    
    parser.add_argument('input_csv', 
                        help='Path to CSV file with scraping results')
    parser.add_argument('--doc-dir', required=True,
                        help='Directory containing scraped documents')
    parser.add_argument('--output-dir', default='.',
                        help='Output directory for reports and charts')
    parser.add_argument('--no-plots', action='store_true',
                        help='Skip generating visualization plots')
    parser.add_argument('--save-details', action='store_true',
                        help='Save detailed CSV reports')
    
    args = parser.parse_args()
    
    print("=" * 85)
    print("  POLICY SCRAPING AUDIT TOOL")
    print("=" * 85)
    
    # Load data
    df = load_policy_data(args.input_csv)
    if df is None:
        return
    
    # Extract all URLs
    reference_columns = ['reference', 'reference2', 'reference3', 'reference4']
    print(f"\n🔍 Extracting URLs from columns: {reference_columns}")
    url_list = extract_all_urls(df, reference_columns)
    
    if not url_list:
        print("✗ No URLs found in reference columns")
        return
    
    print(f"✓ Found {len(url_list)} total URL references")
    
    # Analyze duplicates
    print(f"\n📊 Analyzing duplicates...")
    duplicate_analysis = analyze_url_duplicates(url_list)
    canonical_analysis = analyze_canonical_duplicates(url_list)
    
    # Analyze errors
    print(f"📊 Analyzing scraping errors...")
    error_analysis = analyze_scraping_errors(url_list)
    
    # Analyze saved documents
    print(f"📊 Analyzing saved documents in '{args.doc_dir}'...")
    doc_analysis = analyze_saved_documents(args.doc_dir, url_list)
    
    if 'error' in doc_analysis:
        print(f"⚠ {doc_analysis['error']}")
    else:
        print(f"✓ Found {doc_analysis['total_saved']:,} saved documents")
        if doc_analysis['empty_files'] > 0:
            print(f"⚠ Warning: {doc_analysis['empty_files']} empty files detected")
    
    # Generate audit report
    report = generate_audit_report(
        url_list=url_list,
        duplicate_analysis=duplicate_analysis,
        canonical_analysis=canonical_analysis,
        error_analysis=error_analysis,
        doc_analysis=doc_analysis
    )
    
    # Print report
    print_audit_report(report, duplicate_analysis, canonical_analysis, 
                      error_analysis, doc_analysis)
    
    # Create visualizations
    if not args.no_plots:
        print(f"\n📊 Generating visualizations...")
        create_visualizations(report, error_analysis, doc_analysis, 
                            output_dir=args.output_dir)
    
    # Save detailed reports
    if args.save_details:
        print(f"\n💾 Saving detailed reports...")
        save_detailed_reports(url_list, duplicate_analysis, error_analysis, 
                            doc_analysis, output_dir=args.output_dir)
    
    # Save summary report
    output_path = Path(args.output_dir) / 'scraping_audit_summary.txt'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("POLICY SCRAPING AUDIT SUMMARY\n")
        f.write("=" * 85 + "\n\n")
        f.write(f"Total URLs: {report['total_urls']:,}\n")
        f.write(f"Unique URLs: {report['unique_urls']:,}\n")
        f.write(f"Saved Documents: {report['saved_documents']:,}\n")
        f.write(f"Missing: {report['missing_documents']:,} ({report['missing_percentage']:.1f}%)\n\n")
        f.write("Reasons for missing documents:\n")
        for i, reason in enumerate(report['reasons'], 1):
            f.write(f"{i}. {reason['reason']}: {reason['count']:,} ({reason['percentage']:.1f}%)\n")
    
    print(f"\n✓ Saved summary report to '{output_path}'")
    
    print("\n" + "=" * 85)
    print("✓ Audit complete!")
    print("=" * 85)
    
    # Print recommendations
    print(f"\n💡 RECOMMENDATIONS")
    print("-" * 85)
    
    if duplicate_analysis['duplicate_instances'] > 100:
        print("  • Implement URL deduplication before scraping to save time and bandwidth")
    
    if canonical_analysis['canonical_duplicate_instances'] > 100:
        print("  • Canonicalize URLs (remove tracking/session parameters) before scraping")
    
    if error_analysis['total_errors'] > 500:
        print("  • Implement retry logic with exponential backoff for failed requests")
        print("  • Consider using proxy rotation to avoid IP blocks")
    
    if error_analysis.get('error_categories', {}).get('403_forbidden', 0) > 100:
        print("  • Many 403 errors suggest IP blocking - use proxy services or slow down")
    
    if error_analysis.get('error_categories', {}).get('timeout', 0) > 100:
        print("  • Many timeouts - increase timeout value or retry failed requests")
    
    if doc_analysis.get('empty_files', 0) > 50:
        print("  • Many empty files - check if content requires JavaScript rendering")
        print("  • Consider using Selenium or Playwright for dynamic content")
    
    if report['unaccounted_for'] > 500:
        print("  • Large number of unaccounted missing documents suggests:")
        print("    - CAPTCHA challenges")
        print("    - Login/paywall requirements")
        print("    - Robots.txt restrictions being enforced")
        print("    - Content requiring authentication")


if __name__ == "__main__":
    main()
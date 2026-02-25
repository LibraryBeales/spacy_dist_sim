"""
Document Character Count Analyzer
Analyzes CSV files containing document metadata and generates:
1. CSV file with IDs of documents under a specified character threshold
2. Histogram visualization of character count distribution
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
from pathlib import Path


def load_data(csv_path):
    """
    Load CSV data from file.
    
    Args:
        csv_path (str): Path to the input CSV file
        
    Returns:
        pd.DataFrame: Loaded dataframe
    """
    try:
        df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(df)} records from {csv_path}")
        return df
    except FileNotFoundError:
        print(f"✗ Error: File '{csv_path}' not found")
        return None
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return None


def extract_document_id(filename):
    """
    Extract document ID from filename (part before underscore).
    
    Args:
        filename (str): Filename string
        
    Returns:
        str: Document ID
    """
    return filename.split('_')[0]


def filter_by_character_count(df, threshold=100):
    """
    Filter documents with character count below threshold.
    
    Args:
        df (pd.DataFrame): Input dataframe
        threshold (int): Character count threshold
        
    Returns:
        pd.DataFrame: Filtered dataframe with IDs
    """
    # Extract IDs
    df['ID'] = df['filename'].apply(extract_document_id)
    
    # Filter by character count
    filtered = df[df['character_count'] < threshold][['ID']].copy()
    
    print(f"\n✓ Found {len(filtered)} documents with < {threshold} characters")
    
    return filtered


def save_filtered_ids(filtered_df, output_path='filtered_document_ids.csv'):
    """
    Save filtered document IDs to CSV file.
    
    Args:
        filtered_df (pd.DataFrame): Dataframe with filtered IDs
        output_path (str): Output CSV file path
    """
    try:
        filtered_df.to_csv(output_path, index=False)
        print(f"✓ Saved filtered IDs to '{output_path}'")
    except Exception as e:
        print(f"✗ Error saving file: {e}")


def create_histogram(df, bin_size=3000, output_path='character_count_histogram.png', 
                     exclude_outliers=False, outlier_threshold=100000):
    """
    Create and save histogram of character counts.
    
    Args:
        df (pd.DataFrame): Input dataframe
        bin_size (int): Size of each histogram bin
        output_path (str): Output image file path
        exclude_outliers (bool): Whether to exclude extreme outliers
        outlier_threshold (int): Threshold for outlier exclusion
    """
    # Get character counts (exclude TOO_SHORT entries)
    char_counts = df[df['character_count'].notna()]['character_count'].values
    
    # Remove outliers if specified
    if exclude_outliers:
        outliers = char_counts[char_counts > outlier_threshold]
        char_counts_filtered = char_counts[char_counts <= outlier_threshold]
        
        if len(outliers) > 0:
            print(f"\n⚠ Excluding {len(outliers)} outlier(s) above {outlier_threshold:,} characters")
            print(f"  Outlier values: {sorted(outliers)}")
    else:
        char_counts_filtered = char_counts
    
    # Calculate statistics
    print(f"\n📊 Character Count Statistics:")
    print(f"  Total documents: {len(df)}")
    print(f"  Mean: {np.mean(char_counts):,.0f} characters")
    print(f"  Median: {np.median(char_counts):,.0f} characters")
    print(f"  Min: {np.min(char_counts):,.0f} characters")
    print(f"  Max: {np.max(char_counts):,.0f} characters")
    print(f"  Std Dev: {np.std(char_counts):,.0f} characters")
    
    # Create bins
    max_count = np.max(char_counts_filtered)
    bins = np.arange(0, max_count + bin_size, bin_size)
    
    # Create figure
    plt.figure(figsize=(14, 8))
    
    # Create histogram
    n, bins_edges, patches = plt.hist(char_counts_filtered, bins=bins, 
                                       edgecolor='black', alpha=0.7, color='steelblue')
    
    # Customize plot
    plt.xlabel('Character Count', fontsize=12, fontweight='bold')
    plt.ylabel('Number of Documents', fontsize=12, fontweight='bold')
    plt.title(f'Document Character Count Distribution (Bin Size: {bin_size:,} characters)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # Format x-axis
    plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    # Add grid
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add statistics text box
    stats_text = f'Total: {len(char_counts_filtered)} docs\n'
    stats_text += f'Mean: {np.mean(char_counts_filtered):,.0f}\n'
    stats_text += f'Median: {np.median(char_counts_filtered):,.0f}'
    
    plt.text(0.98, 0.97, stats_text, transform=plt.gca().transAxes,
             fontsize=10, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    # Tight layout
    plt.tight_layout()
    
    # Save figure
    try:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"\n✓ Saved histogram to '{output_path}'")
    except Exception as e:
        print(f"\n✗ Error saving histogram: {e}")
    
    plt.close()


def print_distribution_summary(df, bin_size=3000, max_bins=20):
    """
    Print text-based distribution summary.
    
    Args:
        df (pd.DataFrame): Input dataframe
        bin_size (int): Size of each bin
        max_bins (int): Maximum number of bins to display
    """
    char_counts = df[df['character_count'].notna()]['character_count'].values
    
    # Create bins
    max_count = np.max(char_counts)
    bins = np.arange(0, min(max_count + bin_size, bin_size * max_bins), bin_size)
    
    # Calculate histogram
    hist, bin_edges = np.histogram(char_counts, bins=bins)
    
    print(f"\n📈 Distribution Summary (first {max_bins} bins):")
    print("=" * 60)
    
    max_bar_length = 50
    max_count_in_hist = np.max(hist)
    
    for i, count in enumerate(hist):
        bin_start = int(bin_edges[i])
        bin_end = int(bin_edges[i + 1]) - 1
        
        # Create bar
        bar_length = int((count / max_count_in_hist) * max_bar_length) if max_count_in_hist > 0 else 0
        bar = '█' * bar_length
        
        print(f"{bin_start:>7,}-{bin_end:<7,} │ {bar} ({int(count)} docs)")


def main():
    """Main execution function."""
    
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Analyze document character counts from CSV file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_documents.py data.csv
  python analyze_documents.py data.csv --threshold 50 --bin-size 5000
  python analyze_documents.py data.csv --exclude-outliers --outlier-threshold 200000
        """
    )
    
    parser.add_argument('input_csv', help='Path to input CSV file')
    parser.add_argument('--threshold', type=int, default=100,
                        help='Character count threshold for filtering (default: 100)')
    parser.add_argument('--bin-size', type=int, default=3000,
                        help='Histogram bin size in characters (default: 3000)')
    parser.add_argument('--output-csv', default='filtered_document_ids.csv',
                        help='Output CSV filename (default: filtered_document_ids.csv)')
    parser.add_argument('--output-plot', default='character_count_histogram.png',
                        help='Output plot filename (default: character_count_histogram.png)')
    parser.add_argument('--exclude-outliers', action='store_true',
                        help='Exclude extreme outliers from histogram')
    parser.add_argument('--outlier-threshold', type=int, default=100000,
                        help='Threshold for outlier exclusion (default: 100000)')
    parser.add_argument('--no-plot', action='store_true',
                        help='Skip generating histogram plot')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("  DOCUMENT CHARACTER COUNT ANALYZER")
    print("=" * 60)
    
    # Load data
    df = load_data(args.input_csv)
    if df is None:
        return
    
    # Validate required columns
    required_columns = ['filename', 'character_count']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Error: Missing required columns: {missing_columns}")
        print(f"  Available columns: {list(df.columns)}")
        return
    
    # Filter documents by character count
    filtered_df = filter_by_character_count(df, threshold=args.threshold)
    
    # Save filtered IDs
    save_filtered_ids(filtered_df, output_path=args.output_csv)
    
    # Print distribution summary
    print_distribution_summary(df, bin_size=args.bin_size)
    
    # Create histogram
    if not args.no_plot:
        create_histogram(df, 
                        bin_size=args.bin_size, 
                        output_path=args.output_plot,
                        exclude_outliers=args.exclude_outliers,
                        outlier_threshold=args.outlier_threshold)
    
    print("\n" + "=" * 60)
    print("✓ Analysis complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
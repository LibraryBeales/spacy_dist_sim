"""
Retry Failed URLs with Cloudflare Bypass (cloudscraper only)
No Chrome/Selenium required - simpler and more reliable
"""

import os
import re
import time
import random
import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from pathlib import Path

# -----------------------
# CONFIG
# -----------------------
FAILED_CSV = "failed_urls_report/all_failed_urls.csv"
OUTPUT_CSV = "retry_results.csv"
OUTPUT_DIR = "scraped_policy_docs2"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Create cloudscraper session
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'mobile': False
    },
    delay=10
)

TIMEOUT = 30
MAX_RETRIES = 3


# -----------------------
# HELPERS
# -----------------------

def clean_text(text):
    """Clean webpage text."""
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def get_file_extension_from_url(url):
    parsed = urlparse(url)
    path = parsed.path.lower()
    if path.endswith(".pdf"):
        return "pdf"
    return None


def download_pdf(url, save_path, retries=0):
    """Download PDF with retry logic."""
    try:
        r = scraper.get(url, timeout=TIMEOUT)
        r.raise_for_status()

        with open(save_path, "wb") as f:
            f.write(r.content)
        
        return True
        
    except Exception as e:
        if retries < MAX_RETRIES:
            print(f"    Retrying ({retries + 1}/{MAX_RETRIES})...")
            time.sleep(random.uniform(5, 10))
            return download_pdf(url, save_path, retries + 1)
        raise


def scrape_html(url, save_path, retries=0):
    """Scrape HTML with retry logic."""
    try:
        r = scraper.get(url, timeout=TIMEOUT)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        # remove junk
        for tag in soup(["script", "style", "nav", "footer"]):
            tag.extract()

        text = soup.get_text(separator=" ")
        text = clean_text(text)
        
        # Check if content is substantial
        if len(text.strip()) < 100:
            raise Exception("Content too short, might be blocked")

        with open(save_path, "w", encoding="utf-8") as f:
            f.write(text)
        
        return True
        
    except Exception as e:
        if retries < MAX_RETRIES:
            print(f"    Retrying ({retries + 1}/{MAX_RETRIES})...")
            time.sleep(random.uniform(5, 10))
            return scrape_html(url, save_path, retries + 1)
        raise


def retry_url(row):
    """Retry downloading a single failed URL."""
    policy_id = row['policy_id']
    ref_number = row['ref_number']
    url = row['url']
    
    if pd.isna(url) or str(url).strip() == "" or url == "N/A":
        return None, "No valid URL"

    url = str(url).strip()

    try:
        ext = get_file_extension_from_url(url)

        # fallback: check headers
        if not ext:
            try:
                head = scraper.head(url, timeout=TIMEOUT, allow_redirects=True)
                content_type = head.headers.get("content-type", "").lower()
                if "pdf" in content_type:
                    ext = "pdf"
                else:
                    ext = "html"
            except:
                ext = "html"

        filename_base = f"{policy_id}_ref{ref_number}"

        if ext == "pdf":
            save_path = os.path.join(OUTPUT_DIR, filename_base + ".pdf")
            download_pdf(url, save_path)
            return "pdf", None

        else:
            save_path = os.path.join(OUTPUT_DIR, filename_base + ".txt")
            scrape_html(url, save_path)
            return "html", None

    except Exception as e:
        return None, str(e)


# -----------------------
# MAIN
# -----------------------

def main():
    print("=" * 85)
    print("  RETRY FAILED URLs WITH CLOUDFLARE BYPASS")
    print("  (Using cloudscraper - no Chrome required)")
    print("=" * 85)
    
    # Load failed URLs
    print(f"\n📂 Loading failed URLs from: {FAILED_CSV}")
    
    try:
        df = pd.read_csv(FAILED_CSV)
    except FileNotFoundError:
        print(f"✗ Error: File '{FAILED_CSV}' not found")
        csv_files = list(Path('.').rglob('*failed*.csv'))
        if csv_files:
            print("\nFound these CSV files:")
            for f in csv_files:
                print(f"  - {f}")
        return
    
    print(f"✓ Loaded {len(df)} failed URLs")
    
    # Show error breakdown
    if 'error_category' in df.columns:
        print("\nError categories:")
        for cat, count in df['error_category'].value_counts().items():
            print(f"  • {count:>4} × {cat}")
    
    # Filter options
    print("\nRetry options:")
    print("  1. Retry all failed URLs")
    print("  2. Retry only 403/Cloudflare errors")
    print("  3. Retry only timeout/connection errors")
    print("  [Enter] Retry all")
    
    choice = input("\nEnter choice (1-3) or press Enter: ").strip()
    
    if choice == "2":
        if 'error' in df.columns:
            df = df[df['error'].str.contains('403|Forbidden|cloudflare', case=False, na=False)]
        elif 'error_category' in df.columns:
            df = df[df['error_category'].str.contains('403|Forbidden', case=False, na=False)]
        print(f"  → Filtered to {len(df)} Cloudflare/403 errors")
    elif choice == "3":
        if 'error' in df.columns:
            df = df[df['error'].str.contains('timeout|connection|timed out', case=False, na=False)]
        elif 'error_category' in df.columns:
            df = df[df['error_category'].str.contains('Timeout|Connection', case=False, na=False)]
        print(f"  → Filtered to {len(df)} timeout/connection errors")
    
    if len(df) == 0:
        print("✗ No URLs to retry")
        return
    
    # Add result columns
    df['retry_status'] = ''
    df['retry_type'] = ''
    df['retry_error'] = ''
    
    total = len(df)
    success_count = 0
    error_count = 0
    
    print(f"\n🚀 Starting retry for {total} URLs...")
    print("=" * 85)
    
    start_time = time.time()
    
    for idx, row in df.iterrows():
        processed = error_count + success_count + 1
        
        policy_id = row['policy_id']
        ref_number = row['ref_number']
        url = row['url']
        original_error = row.get('error', 'Unknown')
        country = row.get('country', 'N/A')
        
        print(f"\n[{processed}/{total}] {policy_id}_ref{ref_number} ({country})")
        print(f"  URL: {str(url)[:70]}...")
        
        filetype, error = retry_url(row)
        
        if filetype:
            df.at[idx, 'retry_status'] = 'SUCCESS'
            df.at[idx, 'retry_type'] = filetype
            success_count += 1
            print(f"  ✅ Success! ({filetype})")
        else:
            df.at[idx, 'retry_status'] = 'FAILED'
            df.at[idx, 'retry_error'] = error
            error_count += 1
            print(f"  ❌ Failed: {str(error)[:60]}...")
        
        success_rate = (success_count / processed * 100) if processed > 0 else 0
        print(f"  Progress: {success_count}/{processed} ({success_rate:.1f}%)")
        
        # Delay between requests
        delay = random.uniform(3, 6)
        time.sleep(delay)
        
        # Save progress every 25 URLs
        if processed % 25 == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            elapsed = time.time() - start_time
            avg_time = elapsed / processed
            remaining = (total - processed) * avg_time
            print(f"\n  💾 Progress saved | ETA: {remaining/60:.1f} min")
    
    # Final save
    df.to_csv(OUTPUT_CSV, index=False)
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 85)
    print("✅ RETRY COMPLETE!")
    print("=" * 85)
    print(f"  Total URLs retried:  {total}")
    print(f"  Successful:          {success_count} ({success_count/total*100:.1f}%)")
    print(f"  Still failed:        {error_count} ({error_count/total*100:.1f}%)")
    print(f"  Time elapsed:        {elapsed/60:.1f} minutes")
    print(f"  Files saved to:      {OUTPUT_DIR}")
    print(f"  Results CSV:         {OUTPUT_CSV}")
    print("=" * 85)
    
    # Show remaining errors
    if error_count > 0:
        print("\n📊 Remaining errors (top 5):")
        remaining_errors = df[df['retry_status'] == 'FAILED']['retry_error'].value_counts().head(5)
        for error, count in remaining_errors.items():
            print(f"  • {count:>4} × {str(error)[:60]}...")


if __name__ == "__main__":
    main()
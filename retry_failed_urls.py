"""
Retry Failed URLs with Cloudflare Bypass
Reads failed_urls CSV and retries scraping with cloudscraper + Selenium fallback
Works with columns: policy_id, reference_column, ref_number, url, error, country, policy_title, error_category
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
FAILED_CSV = "failed_urls_report/all_failed_urls.csv"  # Your failed URLs CSV
OUTPUT_CSV = "retry_results.csv"
OUTPUT_DIR = "scraped_policy_docs2"  # Same directory as before

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

# Track which domains need Selenium
SELENIUM_DOMAINS = set()


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


def get_domain(url):
    """Extract domain from URL."""
    return urlparse(url).netloc


def scrape_with_selenium_fallback(url, save_path):
    """Fallback to Selenium for stubborn Cloudflare sites."""
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.common.by import By
        
        print("    → Using Selenium for Cloudflare bypass...")
        
        options = uc.ChromeOptions()
        options.add_argument('--headless=new')  # Updated headless mode
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--remote-debugging-port=9222')  # Add this
        
        # Let it auto-detect Chrome version
        driver = uc.Chrome(options=options, use_subprocess=True)
        
        try:
            driver.get(url)
            time.sleep(8)
            
            # Check for Cloudflare
            page_source = driver.page_source
            if "Just a moment" in page_source:
                print("    → Waiting for Cloudflare...")
                time.sleep(12)
            
            text = driver.find_element(By.TAG_NAME, 'body').text
            
            if len(text.strip()) < 100:
                raise Exception("Content too short")
            
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(text)
            
            return True
            
        finally:
            driver.quit()
            
    except Exception as e:
        print(f"    ✗ Selenium error: {str(e)[:60]}...")
        raise

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
    """Scrape HTML with retry logic and Selenium fallback."""
    domain = get_domain(url)
    
    # Use Selenium directly if we know this domain needs it
    if domain in SELENIUM_DOMAINS:
        return scrape_with_selenium_fallback(url, save_path)
    
    try:
        r = scraper.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        
        # Check if we got a Cloudflare challenge page
        if "Just a moment" in r.text or "Checking your browser" in r.text or "cf-browser-verification" in r.text:
            print("    → Cloudflare detected, switching to Selenium...")
            SELENIUM_DOMAINS.add(domain)
            return scrape_with_selenium_fallback(url, save_path)

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
        error_str = str(e)
        
        # Try Selenium as fallback for 403/Cloudflare errors
        if any(keyword in error_str.lower() for keyword in ['403', 'forbidden', 'cloudflare', 'blocked']):
            if retries == 0:  # Only try Selenium once
                print(f"    → {error_str[:50]}... trying Selenium...")
                SELENIUM_DOMAINS.add(domain)
                try:
                    return scrape_with_selenium_fallback(url, save_path)
                except Exception as selenium_error:
                    print(f"    → Selenium also failed: {str(selenium_error)[:50]}...")
                    raise
        
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
                # If HEAD fails, assume HTML
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
    print("=" * 85)
    
    # Load failed URLs
    print(f"\n📂 Loading failed URLs from: {FAILED_CSV}")
    
    try:
        df = pd.read_csv(FAILED_CSV)
    except FileNotFoundError:
        print(f"✗ Error: File '{FAILED_CSV}' not found")
        print("\nLooking for CSV files...")
        
        # Search for possible CSV files
        csv_files = list(Path('.').rglob('*failed*.csv'))
        if csv_files:
            print("\nFound these CSV files:")
            for i, f in enumerate(csv_files, 1):
                print(f"  {i}. {f}")
            print("\nPlease update FAILED_CSV variable with correct path")
        return
    
    print(f"✓ Loaded {len(df)} failed URLs")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Verify required columns
    required_cols = ['policy_id', 'ref_number', 'url']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"✗ Error: Missing required columns: {missing_cols}")
        return
    
    # Filter out URLs that shouldn't be retried (optional)
    print("\n🔍 Analyzing which URLs to retry...")
    
    # Count by error category
    if 'error_category' in df.columns:
        category_summary = df['error_category'].value_counts()
        print("\nError categories:")
        for category, count in category_summary.items():
            print(f"  • {count:>4} × {category}")
    
    # Count by error message
    if 'error' in df.columns:
        error_summary = df['error'].value_counts().head(5)
        print("\nTop error messages:")
        for error, count in error_summary.items():
            print(f"  • {count:>4} × {str(error)[:60]}...")
    
    # Option to filter by error type
    print(f"\n📋 Total URLs to retry: {len(df)}")
    
    # Ask user if they want to filter
    print("\nRetry options:")
    print("  1. Retry all failed URLs")
    print("  2. Retry only 403/Cloudflare errors")
    print("  3. Retry only timeout/connection errors")
    print("  4. Retry by error category")
    print("  [Enter] Skip filtering")
    
    choice = input("\nEnter choice (1-4) or press Enter: ").strip()
    
    original_count = len(df)
    
    if choice == "2":
        if 'error' in df.columns:
            df = df[df['error'].str.contains('403|Forbidden|cloudflare', case=False, na=False)]
            print(f"  → Filtered to {len(df)} Cloudflare/403 errors")
        elif 'error_category' in df.columns:
            df = df[df['error_category'].str.contains('403', case=False, na=False)]
            print(f"  → Filtered to {len(df)} 403 errors")
    elif choice == "3":
        if 'error' in df.columns:
            df = df[df['error'].str.contains('timeout|connection|timed out', case=False, na=False)]
            print(f"  → Filtered to {len(df)} timeout/connection errors")
        elif 'error_category' in df.columns:
            df = df[df['error_category'].str.contains('timeout|connection', case=False, na=False)]
            print(f"  → Filtered to {len(df)} timeout/connection errors")
    elif choice == "4" and 'error_category' in df.columns:
        print("\nAvailable categories:")
        for i, cat in enumerate(df['error_category'].unique(), 1):
            print(f"  {i}. {cat}")
        cat_choice = input("\nEnter category name to retry: ").strip()
        df = df[df['error_category'] == cat_choice]
        print(f"  → Filtered to {len(df)} URLs in category '{cat_choice}'")
    
    if len(df) == 0:
        print("✗ No URLs to retry after filtering")
        return
    
    # Add result columns
    df['retry_status'] = ''
    df['retry_type'] = ''
    df['retry_error'] = ''
    
    total = len(df)
    success_count = 0
    error_count = 0
    
    print(f"\n🚀 Starting retry process for {total} URLs...")
    print("=" * 85)
    
    start_time = time.time()
    
    for idx, row in df.iterrows():
        processed = error_count + success_count + 1
        
        policy_id = row['policy_id']
        ref_number = row['ref_number']
        url = row['url']
        original_error = row.get('error', 'Unknown')
        country = row.get('country', 'N/A')
        
        print(f"\n[{processed}/{total}] {policy_id}_ref{ref_number}")
        print(f"  Country: {country}")
        print(f"  URL: {str(url)[:70]}...")
        print(f"  Original error: {str(original_error)[:60]}...")
        
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
        
        # Progress update
        success_rate = (success_count / processed * 100) if processed > 0 else 0
        print(f"  Progress: {success_count}/{processed} successful ({success_rate:.1f}%)")
        
        # Longer delay for Cloudflare
        delay = random.uniform(4, 8)
        print(f"  ⏳ Waiting {delay:.1f}s...")
        time.sleep(delay)
        
        # Save progress every 25 URLs
        if processed % 25 == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            elapsed = time.time() - start_time
            avg_time = elapsed / processed
            remaining = (total - processed) * avg_time
            print(f"\n  💾 Progress saved to {OUTPUT_CSV}")
            print(f"  ⏱️  Estimated time remaining: {remaining/60:.1f} minutes")
    
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
    print(f"  Avg time per URL:    {elapsed/total:.1f} seconds")
    print(f"  Files saved to:      {OUTPUT_DIR}")
    print(f"  Results CSV:         {OUTPUT_CSV}")
    print("=" * 85)
    
    # Show breakdown of remaining errors
    if error_count > 0:
        print("\n📊 Remaining errors breakdown:")
        remaining_errors = df[df['retry_status'] == 'FAILED']['retry_error'].value_counts().head(5)
        for error, count in remaining_errors.items():
            print(f"  • {count:>4} × {str(error)[:60]}...")
    
    # Domains that needed Selenium
    if SELENIUM_DOMAINS:
        print(f"\n🔧 Domains that required Selenium bypass:")
        for domain in SELENIUM_DOMAINS:
            print(f"  • {domain}")
    
    # Show success by country
    if 'country' in df.columns and success_count > 0:
        print(f"\n🌍 Success by country (top 10):")
        success_df = df[df['retry_status'] == 'SUCCESS']
        country_success = success_df['country'].value_counts().head(10)
        for country, count in country_success.items():
            print(f"  • {country}: {count}")


if __name__ == "__main__":
    main()
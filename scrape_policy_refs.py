import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# -----------------------
# CONFIG
# -----------------------
INPUT_CSV = "policies.csv"
OUTPUT_CSV = "policies_with_downloads2.csv"
OUTPUT_DIR = "scraped_policy_docs2"

os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; policy-scraper/1.0)"
}

REFERENCE_COLUMNS = [
    "reference",
    "reference2",
    "reference3",
    "reference4"
]

TIMEOUT = 30


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


def download_pdf(url, save_path):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    with open(save_path, "wb") as f:
        f.write(r.content)


def scrape_html(url, save_path):
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # remove junk
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.extract()

    text = soup.get_text(separator=" ")
    text = clean_text(text)

    with open(save_path, "w", encoding="utf-8") as f:
        f.write(text)


def process_url(url, policy_id, ref_num):
    """Download or scrape a single URL."""
    if pd.isna(url) or str(url).strip() == "":
        return None, None  # no type, no error

    url = str(url).strip()

    try:
        ext = get_file_extension_from_url(url)

        # fallback: check headers
        if not ext:
            head = requests.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
            content_type = head.headers.get("content-type", "").lower()
            if "pdf" in content_type:
                ext = "pdf"
            else:
                ext = "html"

        filename_base = f"{policy_id}_ref{ref_num}"

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

df = pd.read_csv(INPUT_CSV)

# create result columns
for i in range(1, 5):
    df[f"ref{i}_type"] = ""
    df[f"ref{i}_error"] = ""

for idx, row in df.iterrows():
    policy_id = row["policy_id"]

    for i, col in enumerate(REFERENCE_COLUMNS, start=1):
        if col not in df.columns:
            continue

        url = row[col]

        filetype, error = process_url(url, policy_id, i)

        if filetype:
            df.at[idx, f"ref{i}_type"] = filetype

        if error:
            df.at[idx, f"ref{i}_error"] = error

        time.sleep(1)  # be polite to servers

df.to_csv(OUTPUT_CSV, index=False)

print("Done.")
print(f"Files saved to: {OUTPUT_DIR}")
print(f"Updated CSV: {OUTPUT_CSV}")

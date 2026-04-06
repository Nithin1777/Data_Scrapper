import requests
import xml.etree.ElementTree as ET
import cloudscraper
from bs4 import BeautifulSoup
import re
import csv
import pandas as pd
import time
import random
import concurrent.futures
from threading import Lock
from datetime import datetime, timedelta
import os

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SITEMAP_URL = "https://robokits.co.in/sitemap/sitemapproducts.xml"
RESULTS_FILE = os.path.join(BASE_DIR, "robokits_normalized.csv")
PROGRESS_FILE = os.path.join(BASE_DIR, "robokits_progress.csv")
MAX_WORKERS = 8

CANONICAL_FIELDS = ["url", "vendor", "title", "sku", "price", "in_stock", "stock_qty", "scraped_at"]
GST_RATE = 1.18

data_lock = Lock()
progress_lock = Lock()


def get_urls_from_sitemap():
    print("[🔗] Fetching sitemap...")
    scraper = cloudscraper.create_scraper()
    r = scraper.get(SITEMAP_URL, timeout=15)
    root = ET.fromstring(r.content)
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = [loc.text for loc in root.findall('ns:url/ns:loc', namespace)]
    print(f"[✅] Found {len(urls)} product URLs")
    return urls


def get_product_info(url):
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, timeout=15)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # --- Title ---
        # Format: "Product Name [RKI-XXXX] - ₹price : Robokits India, ..."
        raw_title = soup.find("title").text
        title_match = re.match(r'^(.+?)\s*\[RKI-', raw_title)
        title = title_match.group(1).strip() if title_match else raw_title.split(" - ₹")[0].strip()

        # --- SKU ---
        sku_elem = soup.find("div", class_="product-info__sku")
        sku = sku_elem.find("strong").text.strip() if sku_elem else None

        # --- Price --- (from title tag, multiply by 1.18 for GST)
        price_match = re.search(r'₹([\d,]+\.?\d*)', raw_title)
        if price_match:
            raw_price = float(price_match.group(1).replace(",", ""))
            price = round(raw_price * GST_RATE, 2)
        else:
            price = None

        # --- Stock ---
        availability_elem = soup.find("div", class_="product-info__availability")
        if availability_elem:
            strong = availability_elem.find("strong")
            stock_text = strong.text.strip() if strong else ""
            in_stock = "In Stock" in stock_text
        else:
            in_stock = None

        return {
            "url": url,
            "vendor": "robokits",
            "title": title,
            "sku": sku,
            "price": price,
            "in_stock": in_stock,
            "stock_qty": None,  # not exposed on product pages
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception as e:
        print(f"[!] Error: {url} → {e}")
        return None


def load_progress():
    try:
        df = pd.read_csv(PROGRESS_FILE)
        return set(df[df['status'] == 'SUCCESS']['url'].tolist())
    except:
        return set()


def save_result(result):
    with data_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CANONICAL_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)


def save_progress(url, status):
    with progress_lock:
        file_exists = os.path.exists(PROGRESS_FILE)
        with open(PROGRESS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["url", "status", "timestamp"])
            writer.writerow([url, status, time.strftime("%Y-%m-%d %H:%M:%S")])


def scrape_all():
    all_urls = get_urls_from_sitemap()
    completed = load_progress()
    remaining = [u for u in all_urls if u not in completed]

    print(f"[📊] Total: {len(all_urls)} | Done: {len(completed)} | Remaining: {len(remaining)}")

    processed = 0
    start_time = time.time()

    def process(url):
        nonlocal processed
        result = get_product_info(url)

        if result:
            save_result(result)
            save_progress(url, "SUCCESS")
            print(f"[✅] {result['sku']} | ₹{result['price']} | {'In Stock' if result['in_stock'] else 'Out of Stock'}")
        else:
            save_progress(url, "FAILED")
            print(f"[❌] Failed: {url}")

        with progress_lock:
            processed += 1
            if processed % 50 == 0:
                pct = (processed / len(remaining)) * 100
                elapsed = time.time() - start_time
                rate = processed / elapsed
                eta_secs = (len(remaining) - processed) / rate
                eta = datetime.now() + timedelta(seconds=eta_secs)
                print(f"[📈] {processed}/{len(remaining)} ({pct:.1f}%) — ETA: {eta.strftime('%H:%M:%S')}")

        time.sleep(random.uniform(0.5, 1.5))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process, remaining)

    print(f"\n[✅] Done! Results in {RESULTS_FILE}")


if __name__ == "__main__":
    scrape_all()
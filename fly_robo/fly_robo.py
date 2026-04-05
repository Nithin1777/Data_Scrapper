import requests
import xml.etree.ElementTree as ET
import cloudscraper
from bs4 import BeautifulSoup
import csv, time, random, os
import concurrent.futures
import pandas as pd
from threading import Lock
from datetime import datetime, timedelta

RESULTS_FILE = "flyrobo_prices.csv"
PROGRESS_FILE = "flyrobo_progress.csv"
MAX_WORKERS = 8
data_lock = Lock()
progress_lock = Lock()

# --- STEP 1: Collect all URLs from 8 sitemaps ---
def get_all_urls():
    scraper = cloudscraper.create_scraper()
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    all_urls = []

    for i in range(1, 9):  # 1 through 8
        sitemap_url = f"https://www.flyrobo.in/sitemap-product-{i}.xml"
        print(f"[🔗] Fetching sitemap {i}/8...")
        try:
            r = scraper.get(sitemap_url, timeout=15)
            root = ET.fromstring(r.content)
            urls = [loc.text for loc in root.findall('ns:url/ns:loc', namespace)]
            all_urls.extend(urls)
            print(f"[✓] Sitemap {i}: {len(urls)} URLs")
            time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            print(f"[!] Error on sitemap {i}: {e}")

    print(f"\n[✅] Total: {len(all_urls)} product URLs")
    return all_urls

# --- STEP 2: Scrape individual product ---
def get_product_info(url):
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, timeout=15)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        price = soup.find("meta", property="product:price:amount")
        availability = soup.find("meta", property="product:availability")
        title = soup.find("meta", property="og:title")

        return {
            "url": url,
            "name": title["content"] if title else "N/A",
            "price": float(price["content"]) if price else None,
            # Note: flyrobo uses "in stock" with space, not "instock"
            "in_stock": availability["content"].strip().lower() == "in stock" if availability else None
        }
    except Exception as e:
        print(f"[!] Error: {url} → {e}")
        return None

# --- STEP 3: Progress tracking (reused pattern) ---
def load_progress():
    try:
        df = pd.read_csv(PROGRESS_FILE)
        return set(df['url'].tolist())
    except:
        return set()

def save_result(result):
    with data_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["url", "name", "price", "in_stock", "scraped_at"])
            writer.writerow([
                result["url"], result["name"],
                result["price"], result["in_stock"],
                time.strftime("%Y-%m-%d %H:%M:%S")
            ])

def save_progress(url, status):
    with progress_lock:
        file_exists = os.path.exists(PROGRESS_FILE)
        with open(PROGRESS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["url", "status", "timestamp"])
            writer.writerow([url, status, time.strftime("%Y-%m-%d %H:%M:%S")])

# --- STEP 4: Main ---
def scrape_all():
    all_urls = get_all_urls()
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
            print(f"[✅] {result['name'][:40]} | ₹{result['price']} | {'In Stock' if result['in_stock'] else 'Out of Stock'}")
        else:
            save_progress(url, "FAILED")
            print(f"[❌] Failed: {url}")

        with progress_lock:
            processed += 1
            if processed % 100 == 0:
                pct = (processed / len(remaining)) * 100
                elapsed = time.time() - start_time
                eta_secs = (len(remaining) - processed) / (processed / elapsed)
                eta = datetime.now() + timedelta(seconds=eta_secs)
                print(f"[📈] {processed}/{len(remaining)} ({pct:.1f}%) — ETA: {eta.strftime('%H:%M:%S')}")

        time.sleep(random.uniform(0.5, 1.5))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process, remaining)

    print(f"\n[✅] Done! Results in {RESULTS_FILE}")

if __name__ == "__main__":
    scrape_all()
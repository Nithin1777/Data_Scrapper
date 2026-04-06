import xml.etree.ElementTree as ET
import cloudscraper
from bs4 import BeautifulSoup
import csv, time, random, os
import concurrent.futures
import pandas as pd
from threading import Lock
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "flyrobo_normalized.csv")
PROGRESS_FILE = os.path.join(BASE_DIR, "flyrobo_progress.csv")
URLS_FILE = os.path.join(BASE_DIR, "flyrobo_urls.csv")

MAX_WORKERS = 8
CANONICAL_FIELDS = ["url", "vendor", "title", "sku", "price", "in_stock", "stock_qty", "scraped_at"]

data_lock = Lock()
progress_lock = Lock()


def get_all_urls():
    if os.path.exists(URLS_FILE):
        df = pd.read_csv(URLS_FILE)
        print(f"[📋] Loaded {len(df)} URLs from cache")
        return df['url'].tolist()

    scraper = cloudscraper.create_scraper()
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    all_urls = []

    for i in range(1, 9):
        sitemap_url = f"https://www.flyrobo.in/sitemap-product-{i}.xml"
        print(f"[🔗] Fetching sitemap {i}/8...")
        try:
            r = scraper.get(sitemap_url, timeout=15)
            if r.status_code != 200 or len(r.content) == 0:
                print(f"[!] Skipping sitemap {i} — status={r.status_code} size={len(r.content)}")
                continue
            root = ET.fromstring(r.content)
            urls = [loc.text for loc in root.findall('ns:url/ns:loc', namespace)]
            all_urls.extend(urls)
            print(f"[✓] Sitemap {i}: {len(urls)} URLs (total: {len(all_urls)})")
            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            print(f"[!] Error on sitemap {i}: {e}")
            continue

    # Only save if we got all 8 sitemaps worth of data
    # Partial saves would cause incomplete scraping runs
    if all_urls:
        with open(URLS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["url"])
            for url in all_urls:
                writer.writerow([url])
        print(f"\n[✅] Saved {len(all_urls)} URLs → {URLS_FILE}")
    else:
        print("[❌] No URLs collected — not saving. Fix sitemap errors and retry.")
        return []

    return all_urls


def get_product_info(url):
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, timeout=15)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        title_tag = soup.find("meta", property="og:title")
        title = title_tag["content"].strip() if title_tag else None

        price_tag = soup.find("meta", property="product:price:amount")
        if price_tag:
            try:
                price = round(float(price_tag["content"]), 2)
            except ValueError:
                price = None
        else:
            price = None

        availability_tag = soup.find("meta", property="product:availability")
        if availability_tag:
            in_stock = availability_tag["content"].strip().lower() == "in stock"
        else:
            in_stock = None

        return {
            "url": url,
            "vendor": "flyrobo",
            "title": title,
            "sku": None,
            "price": price,
            "in_stock": in_stock,
            "stock_qty": None,
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
    all_urls = get_all_urls()
    if not all_urls:
        print("[❌] No URLs to scrape. Exiting.")
        return

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
            print(f"[✅] {str(result['title'])[:40]} | ₹{result['price']} | {'In Stock' if result['in_stock'] else 'Out of Stock'}")
        else:
            save_progress(url, "FAILED")
            print(f"[❌] Failed: {url}")

        with progress_lock:
            processed += 1
            if processed % 100 == 0:
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
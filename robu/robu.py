import xml.etree.ElementTree as ET
import csv, time, random, os
import threading
import concurrent.futures
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from threading import Lock
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "robu_normalized.csv")
PROGRESS_FILE = os.path.join(BASE_DIR, "robu_progress.csv")
URLS_FILE = os.path.join(BASE_DIR, "robu_urls.csv")

MAX_WORKERS = 30 #keep at 30 higher than this and the server will throttle
CANONICAL_FIELDS = ["url", "vendor", "title", "sku", "price", "in_stock", "stock_qty", "scraped_at"]

data_lock = Lock()
progress_lock = Lock()
thread_local = threading.local()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]


def get_scraper():
    if not hasattr(thread_local, "scraper"):
        thread_local.scraper = cloudscraper.create_scraper()
        thread_local.scraper.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    return thread_local.scraper


# --- STEP 1: Collect all URLs from sitemaps ---

def get_all_urls(total_sitemaps=493):
    # Skip if already collected
    if os.path.exists(URLS_FILE):
        df = pd.read_csv(URLS_FILE)
        print(f"[📋] URLs file exists — loaded {len(df)} URLs from {URLS_FILE}")
        return df['url'].tolist()

    scraper = cloudscraper.create_scraper()
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    all_urls = []

    for i in range(1, total_sitemaps + 1):
        url = f"https://robu.in/product-sitemap{i}.xml"
        print(f"[🔗] Fetching sitemap {i}/{total_sitemaps}...")
        try:
            r = scraper.get(url, timeout=15)
            if r.status_code != 200:
                print(f"[!] HTTP {r.status_code} for sitemap {i}, skipping...")
                continue
            root = ET.fromstring(r.content)
            urls = [loc.text for loc in root.findall('ns:url/ns:loc', namespace)]
            all_urls.extend(urls)
            print(f"[✓] Sitemap {i}: {len(urls)} URLs (total: {len(all_urls)})")
            time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            print(f"[!] Error on sitemap {i}: {e}")
            continue

    # Save for reuse
    with open(URLS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url"])
        for url in all_urls:
            writer.writerow([url])

    print(f"\n[✅] Collected {len(all_urls)} URLs → {URLS_FILE}")
    return all_urls


# --- STEP 2: Scrape individual product ---

def get_product_info(url):
    scraper = get_scraper()
    try:
        r = scraper.get(url, timeout=15)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Title — og:image:alt is the cleanest source
        title_tag = soup.find("meta", property="og:image:alt")
        title = title_tag["content"].strip() if title_tag else None

        # Fallback to og:title if image:alt missing
        if not title:
            og_title = soup.find("meta", property="og:title")
            title = og_title["content"].strip() if og_title else None

        # Price — already GST inclusive
        price_tag = soup.find("meta", property="product:price:amount")
        if price_tag:
            try:
                price = round(float(price_tag["content"]), 2)
            except ValueError:
                price = None
        else:
            price = None

        # Stock
        availability_tag = soup.find("meta", property="product:availability")
        if availability_tag:
            in_stock = availability_tag["content"].strip().lower() == "instock"
        else:
            in_stock = None

        return {
            "url": url,
            "vendor": "robu",
            "title": title,
            "sku": None,  # not exposed
            "price": price,
            "in_stock": in_stock,
            "stock_qty": None,  # not exposed
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception as e:
        print(f"[!] Error fetching {url}: {e}")
        return None


# --- STEP 3: Progress tracking ---

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


# --- STEP 4: Main ---

def scrape_all():
    all_urls = get_all_urls()
    completed = load_progress()
    remaining = [u for u in all_urls if u not in completed]

    print(f"[📊] Total: {len(all_urls)} | Done: {len(completed)} | Remaining: {len(remaining)}")

    processed = 0
    start_time = time.time()

    def process_url(url):
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

        time.sleep(random.uniform(0.2, 0.8))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_url, remaining)

    print(f"\n[✅] All done! Results saved to {RESULTS_FILE}")


if __name__ == "__main__":
    scrape_all()
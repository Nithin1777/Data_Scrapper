import pandas as pd
import concurrent.futures
import csv
import time
import random
import os
import threading
import cloudscraper
from bs4 import BeautifulSoup
from threading import Lock
from datetime import datetime, timedelta

RESULTS_FILE = "robu_prices.csv"
PROGRESS_FILE = "robu_progress.csv"
MAX_WORKERS = 40  # Lower is fine — sessions are reused now

data_lock = Lock()
progress_lock = Lock()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

# One persistent scraper per thread — fixes WinError 10055 socket exhaustion
thread_local = threading.local()

def get_scraper():
    if not hasattr(thread_local, "scraper"):
        thread_local.scraper = cloudscraper.create_scraper()
        thread_local.scraper.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    return thread_local.scraper

def get_product_info(url):
    scraper = get_scraper()  # reuse, don't recreate
    try:
        r = scraper.get(url, timeout=15)
        if r.status_code != 200:
            return None
            
        soup = BeautifulSoup(r.text, "html.parser")
        price = soup.find("meta", property="product:price:amount")
        availability = soup.find("meta", property="product:availability")
        
        return {
            "url": url,
            "price": price["content"] if price else "N/A",
            "in_stock": availability["content"] == "instock" if availability else None
        }
    except Exception as e:
        print(f"[!] Error fetching {url}: {e}")
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
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["url", "price", "in_stock"])
            writer.writerow([result["url"], result["price"], result["in_stock"]])

def save_progress(url, status):
    with progress_lock:
        file_exists = os.path.exists(PROGRESS_FILE)
        with open(PROGRESS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["url", "status", "timestamp"])
            writer.writerow([url, status, time.strftime("%Y-%m-%d %H:%M:%S")])

def scrape_all(urls_file="product_urls.csv", max_workers=MAX_WORKERS):
    df = pd.read_csv(urls_file)
    all_urls = df['url'].tolist()
    
    completed = load_progress()
    remaining = [u for u in all_urls if u not in completed]
    
    print(f"[📊] Total: {len(all_urls)} | Completed: {len(completed)} | Remaining: {len(remaining)}")
    
    processed = 0
    start_time = time.time()
    
    def process_url(url):
        nonlocal processed
        result = get_product_info(url)
        
        if result:
            save_result(result)
            save_progress(url, "SUCCESS")
            print(f"[✅] {url} → ₹{result['price']} | {'In Stock' if result['in_stock'] else 'Out of Stock'}")
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
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_url, remaining)
    
    print(f"\n[✅] All done! Results saved to {RESULTS_FILE}")

scrape_all()
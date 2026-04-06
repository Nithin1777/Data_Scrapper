import cloudscraper
from bs4 import BeautifulSoup
import re
import csv
import time
import random
import os
from datetime import datetime, timedelta
from threading import Lock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "evelta_normalized.csv")
PROGRESS_FILE = os.path.join(BASE_DIR, "evelta_progress.csv")

CANONICAL_FIELDS = ["url", "vendor", "title", "sku", "price", "in_stock", "stock_qty", "scraped_at"]
GST_RATE = 1.18

data_lock = Lock()
progress_lock = Lock()

CATEGORY_URLS = [
    "https://evelta.com/integrated-circuits-ics/",
    "https://evelta.com/boards-kits-and-programmers/",
    "https://evelta.com/drone-parts/",
    "https://evelta.com/breakout-boards/",
    "https://evelta.com/categories/communication/",
    "https://evelta.com/categories/passive-components/",
    "https://evelta.com/categories/sensors/",
    "https://evelta.com/categories/connectors/",
    "https://evelta.com/categories/optoelectronics/",
    "https://evelta.com/categories/electromechanical/",
    "https://evelta.com/categories/discrete-semiconductors/",
    "https://evelta.com/3d-printers-and-filaments/",
    "https://evelta.com/wire-and-cable-management/",
    "https://evelta.com/categories/other-components/circuit-protection/",
    "https://evelta.com/power-supplies/",
    "https://evelta.com/test-and-measurement/",
    "https://evelta.com/tools-and-supplies/",
]


def parse_cards(html, seen_urls):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("article.card")
    results = []

    for card in cards:
        try:
            link = card.select_one("a.card-figure-link")
            url = link["href"] if link else None
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            title_a = card.select_one("h4.card-title a")
            title = title_a.text.strip() if title_a else None

            sku_elem = card.select_one("p.card-text--sku")
            sku = sku_elem.text.strip().replace("SKU:", "").strip() if sku_elem else None

            price_elem = card.select_one("span[data-product-price-without-tax]")
            if price_elem:
                raw = re.sub(r"[₹,\s]", "", price_elem.text.strip())
                try:
                    price = round(float(raw) * GST_RATE, 2)
                except ValueError:
                    price = None
            else:
                price = None

            stock_elem = card.select_one("div.card-stock")
            if stock_elem:
                stock_text = stock_elem.text.strip().lower()
                qty_match = re.search(r"(\d+)", stock_text)
                if qty_match:
                    stock_qty = int(qty_match.group(1))
                    in_stock = True
                elif "out of stock" in stock_text:
                    stock_qty = 0
                    in_stock = False
                else:
                    stock_qty = None
                    in_stock = True
            else:
                stock_qty = None
                in_stock = None

            results.append({
                "url": url,
                "vendor": "evelta",
                "title": title,
                "sku": sku,
                "price": price,
                "in_stock": in_stock,
                "stock_qty": stock_qty,
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        except Exception as e:
            print(f"[!] Card parse error: {e}")
            continue

    return results


def scrape_category(category_url, scraper):
    all_results = []
    # seen_urls is global across pages for this category to catch BC redirect quirk
    seen_urls = set()
    page = 1

    while True:
        url = f"{category_url.rstrip('/')}?page={page}"
        try:
            r = scraper.get(url, timeout=15)
            if r.status_code != 200:
                print(f"[!] HTTP {r.status_code} → {url}")
                break

            products = parse_cards(r.text, seen_urls)

            if not products:
                # Empty page = end of pagination
                break

            slug = category_url.split("evelta.com/")[-1].strip("/")
            print(f"  [page {page}] {slug} → {len(products)} products")
            all_results.extend(products)
            page += 1
            time.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            print(f"[!] Error on {url}: {e}")
            break

    return all_results


def load_completed_categories():
    try:
        completed = set()
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["status"] == "SUCCESS":
                    completed.add(row["category"])
        return completed
    except FileNotFoundError:
        return set()


def save_results_batch(results):
    with data_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CANONICAL_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(results)


def save_category_progress(category_url, status, count):
    with progress_lock:
        file_exists = os.path.exists(PROGRESS_FILE)
        with open(PROGRESS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["category", "status", "products_found", "timestamp"])
            writer.writerow([category_url, status, count, time.strftime("%Y-%m-%d %H:%M:%S")])


def scrape_all():
    scraper = cloudscraper.create_scraper()
    completed = load_completed_categories()
    remaining = [c for c in CATEGORY_URLS if c not in completed]

    print(f"[📊] {len(CATEGORY_URLS)} categories | {len(completed)} done | {len(remaining)} remaining\n")

    total_products = 0
    start_time = time.time()

    for i, cat_url in enumerate(remaining, 1):
        slug = cat_url.split("evelta.com/")[-1].strip("/")
        print(f"\n[{i}/{len(remaining)}] {slug}")

        results = scrape_category(cat_url, scraper)

        if results:
            save_results_batch(results)
            save_category_progress(cat_url, "SUCCESS", len(results))
            total_products += len(results)
            print(f"  [✅] {len(results)} products saved")
        else:
            save_category_progress(cat_url, "EMPTY", 0)
            print(f"  [⚠️] No products found — check if URL is valid")

        elapsed = time.time() - start_time
        rate = i / elapsed
        eta_secs = (len(remaining) - i) / rate if rate > 0 else 0
        eta = datetime.now() + timedelta(seconds=eta_secs)
        print(f"  [eta] {total_products} total products | ETA {eta.strftime('%H:%M:%S')}")

        time.sleep(random.uniform(1.5, 2.5))

    print(f"\n[done] {total_products} products → {RESULTS_FILE}")


if __name__ == "__main__":
    scrape_all()
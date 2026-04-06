import cloudscraper
import csv, time, random, os
from threading import Lock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "quartz_normalized.csv")

CANONICAL_FIELDS = ["url", "vendor", "title", "sku", "price", "in_stock", "stock_qty", "scraped_at"]
GST_RATE = 1.18
data_lock = Lock()


def get_all_products_via_api():
    scraper = cloudscraper.create_scraper()
    all_products = []
    page = 1

    while True:
        url = f"https://quartzcomponents.com/products.json?limit=250&page={page}"
        print(f"[🔗] Fetching page {page}...")
        try:
            r = scraper.get(url, timeout=15)
            data = r.json()
            products = data.get("products", [])
            if not products:
                print(f"[✅] No more products at page {page}, done.")
                break
            all_products.extend(products)
            print(f"[✓] Page {page}: {len(products)} products (total: {len(all_products)})")
            page += 1
            time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            print(f"[!] Error on page {page}: {e}")
            break

    return all_products


def parse_product(product):
    """
    One canonical row per product (not per variant).
    Price logic:
      - Take the first available variant's price (sale price if on sale)
      - Multiply by 1.18 for GST
      - If multiple variants exist, we take the cheapest available one
    """
    url = f"https://quartzcomponents.com/products/{product['handle']}"
    title = product.get("title")

    variants = product.get("variants", [])
    if not variants:
        return None

    # Pick cheapest available variant, fallback to cheapest overall
    available = [v for v in variants if v.get("available", False)]
    candidates = available if available else variants
    best = min(candidates, key=lambda v: float(v.get("price", 0)))

    raw_price = float(best.get("price", 0))
    price = round(raw_price * GST_RATE, 2)

    sku = best.get("sku") or None
    in_stock = best.get("available", False)
    stock_qty = best.get("inventory_quantity", None)

    return {
        "url": url,
        "vendor": "quartz",
        "title": title,
        "sku": sku,
        "price": price,
        "in_stock": in_stock,
        "stock_qty": stock_qty if stock_qty is not None else None,
        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def save_results(rows):
    with data_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CANONICAL_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)


def scrape_all():
    products = get_all_products_via_api()
    print(f"\n[📊] Parsing {len(products)} products...")

    rows = []
    skipped = 0
    for product in products:
        row = parse_product(product)
        if row:
            rows.append(row)
        else:
            skipped += 1

    if rows:
        save_results(rows)

    print(f"[✅] Done! {len(rows)} products saved → {RESULTS_FILE}")
    if skipped:
        print(f"[⚠️] {skipped} products skipped (no variants)")


if __name__ == "__main__":
    scrape_all()
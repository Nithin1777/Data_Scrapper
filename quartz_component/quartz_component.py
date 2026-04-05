import cloudscraper
import csv, time, random, os
import pandas as pd
from threading import Lock
from datetime import datetime, timedelta
import concurrent.futures

RESULTS_FILE = "quartz_prices.csv"
data_lock = Lock()

def get_all_products_via_api():
    """Shopify exposes all products via /products.json — no sitemap needed"""
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
    """Extract all useful fields from Shopify product JSON"""
    results = []

    for variant in product.get("variants", []):
        price = float(variant.get("price", 0))
        compare_price = variant.get("compare_at_price")

        results.append({
            "product_id":       product["id"],
            "title":            product["title"],
            "url":              f"https://quartzcomponents.com/products/{product['handle']}",
            "sku":              variant.get("sku", ""),
            "variant_title":    variant.get("title", ""),  # e.g. "10pcs / Blue"
            "price":            price,
            "compare_price":    float(compare_price) if compare_price else None,
            "on_sale":          compare_price is not None and float(compare_price) > price,
            "discount_pct":     round((1 - price / float(compare_price)) * 100, 1)
                                if compare_price and float(compare_price) > 0 else 0,
            "in_stock":         variant.get("available", False),
            "stock_qty":        variant.get("inventory_quantity", 0),
            "scraped_at":       time.strftime("%Y-%m-%d %H:%M:%S")
        })

    return results

def save_results(rows):
    with data_lock:
        file_exists = os.path.exists(RESULTS_FILE)
        with open(RESULTS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

def scrape_all():
    # Step 1: get all products via API
    products = get_all_products_via_api()
    print(f"\n[📊] Parsing {len(products)} products...")

    # Step 2: parse and save
    total_variants = 0
    for product in products:
        rows = parse_product(product)
        if rows:
            save_results(rows)
            total_variants += len(rows)

    print(f"[✅] Done! {len(products)} products, {total_variants} variants → {RESULTS_FILE}")

if __name__ == "__main__":
    scrape_all()
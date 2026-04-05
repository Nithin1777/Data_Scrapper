import cloudscraper
from bs4 import BeautifulSoup
import re
import csv
import time
import random
import os
from datetime import datetime, timedelta
from threading import Lock

# --- CONFIG ---
RESULTS_FILE = "evelta_prices.csv"
PROGRESS_FILE = "evelta_progress.csv"

data_lock = Lock()
progress_lock = Lock()

CATEGORY_URLS = [
    # Integrated Circuits
    "https://evelta.com/integrated-circuits-ics/microcontrollers/",
    "https://evelta.com/integrated-circuits-ics/power-management-ics/battery-management/",
    "https://evelta.com/supervisory-circuits",
    "https://evelta.com/integrated-circuits-ics/power-management-ics/voltage-references/",
    "https://evelta.com/integrated-circuits-ics/power-management-ics/voltage-regulators---linear/",
    "https://evelta.com/integrated-circuits-ics/power-management-ics/voltage-regulators-switching/",
    "https://evelta.com/integrated-circuits-ics/power-management-ics/motor-drivers/",
    "https://evelta.com/integrated-circuits-ics/data-converter-ics/",
    "https://evelta.com/integrated-circuits-ics/clock-and-timing/",
    "https://evelta.com/integrated-circuits-ics/interface-ics/",
    "https://evelta.com/integrated-circuits-ics/linear-amplifier/",
    "https://evelta.com/categories/integrated-circuits-ics/logic-ics/",
    "https://evelta.com/categories/integrated-circuits-ics/memory/",
    "https://evelta.com/categories/integrated-circuits-ics/rf-integrated-circuits/",
    "https://evelta.com/integrated-circuits-ics/fpga-field-programmable-gate-array/",
    "https://evelta.com/categories/integrated-circuits-ics/other-ics/",
    # Development Boards
    "https://evelta.com/development-boards-and-kits/arduino-and-compatible-boards/",
    "https://evelta.com/development-boards-and-kits/raspberry-pi-and-accessories/",
    "https://evelta.com/boards-kits-and-programmers/programmers-emulators-and-debuggers/",
    "https://evelta.com/boards-kits-and-programmers/rf-evaluation-and-development-kits/",
    "https://evelta.com/development-boards-and-kits/evaluation-boards-processors-and-microcontrollers/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-mcu-dsp/arm-development-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-mcu-dsp/avr-development-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-mcu-dsp/msp-development-boards/",
    "https://evelta.com/development-boards-and-kits/evaluation-boards-mcu-and-dsp/pic-and-dspic-development-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-mcu-dsp/pmic-development-tools/",
    "https://evelta.com/development-boards-and-kits/evaluation-boards---mcu-and-dsp/risc-v-development-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-mcu-dsp/x86-development-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-sensors/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-and-demonstration-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-dc-dc-and-ac-dc/",
    "https://evelta.com/categories/boards-kits-and-programmers/evaluation-boards-expansion-boards/",
    "https://evelta.com/categories/boards-kits-and-programmers/single-board-computers/",
    "https://evelta.com/categories/boards-kits-and-programmers/starter-kits/",
    "https://evelta.com/categories/boards-kits-and-programmers/accessories/",
    # Drone Parts
    "https://evelta.com/drone-components/drone-accessories/",
    "https://evelta.com/drone-components/drone-frame/",
    "https://evelta.com/drone-parts/drone-gimbals/",
    "https://evelta.com/drone-parts/drone-gps-modules/",
    "https://evelta.com/drone-components/drone-kit/",
    "https://evelta.com/drone-components/drone-motor/",
    "https://evelta.com/drone-components/drone-propeller/",
    "https://evelta.com/drone-components/drone-remote/",
    "https://evelta.com/drone-components/esc-electronic-speed-controller/",
]


def parse_cards(html, seen_urls):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("article.card")
    results = []

    for card in cards:
        try:
            # URL + duplicate guard (handles BC infinite page redirect quirk)
            link = card.select_one("a.card-figure-link")
            url = link["href"] if link else None
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            # product_id from the Add to Cart hidden input
            pid_input = card.select_one("input[name='product_id']")
            product_id = pid_input["value"] if pid_input else "N/A"

            # title
            title_a = card.select_one("h4.card-title a")
            title = title_a.text.strip() if title_a else "N/A"

            # sku
            sku_elem = card.select_one("p.card-text--sku")
            sku = sku_elem.text.strip().replace("SKU:", "").strip() if sku_elem else "N/A"

            # price excl. GST
            price_elem = card.select_one("span[data-product-price-without-tax]")
            price = re.sub(r"[₹,\s]", "", price_elem.text.strip()) if price_elem else "N/A"

            # stock
            stock_elem = card.select_one("div.card-stock")
            if stock_elem:
                stock_text = stock_elem.text.strip()
                qty_match = re.search(r"(\d+)", stock_text)
                if qty_match:
                    stock_qty = int(qty_match.group(1))
                    in_stock = True
                elif "out of stock" in stock_text.lower():
                    stock_qty = 0
                    in_stock = False
                else:
                    stock_qty = ""
                    in_stock = True
            else:
                stock_qty = ""
                in_stock = ""

            results.append({
                "product_id": product_id,
                "title": title,
                "url": url,
                "sku": sku,
                "variant_title": "",   # not exposed on listing cards
                "price": price,
                "in_stock": in_stock,
                "stock_qty": stock_qty,
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })

        except Exception as e:
            print(f"[!] Card parse error: {e}")
            continue

    return results


def scrape_category(category_url):
    scraper = cloudscraper.create_scraper()
    all_results = []
    seen_urls = set()  # duplicate guard per category
    page = 1

    while True:
        url = f"{category_url.rstrip('/')}?page={page}"
        try:
            r = scraper.get(url, timeout=15)
            if r.status_code != 200:
                print(f"[!] HTTP {r.status_code} -> {url}")
                break

            products = parse_cards(r.text, seen_urls)

            if not products:
                break

            slug = category_url.split("evelta.com/")[-1].strip("/")
            print(f"[page] {slug} | p{page} -> {len(products)} products")
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
            writer = csv.DictWriter(f, fieldnames=[
                "product_id", "title", "url", "sku", "variant_title",
                "price", "in_stock", "stock_qty", "scraped_at"
            ])
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
    completed_categories = load_completed_categories()
    remaining = [c for c in CATEGORY_URLS if c not in completed_categories]

    print(f"[info] {len(CATEGORY_URLS)} categories | {len(completed_categories)} done | {len(remaining)} remaining\n")

    total_products = 0
    start_time = time.time()

    for i, cat_url in enumerate(remaining, 1):
        slug = cat_url.split("evelta.com/")[-1].strip("/")
        print(f"\n[scan] [{i}/{len(remaining)}] {slug}")

        results = scrape_category(cat_url)

        if results:
            save_results_batch(results)
            save_category_progress(cat_url, "SUCCESS", len(results))
            total_products += len(results)
            print(f"[ok]   {len(results)} products saved")
        else:
            save_category_progress(cat_url, "EMPTY", 0)
            print(f"[warn] No products found")

        elapsed = time.time() - start_time
        rate = i / elapsed
        eta_secs = (len(remaining) - i) / rate if rate > 0 else 0
        eta = datetime.now() + timedelta(seconds=eta_secs)
        print(f"[eta]  {i}/{len(remaining)} | {total_products} products | ETA: {eta.strftime('%H:%M:%S')}")

        time.sleep(random.uniform(1.0, 2.0))

    print(f"\n[done] {total_products} products -> {RESULTS_FILE}")


if __name__ == "__main__":
    scrape_all()
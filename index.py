import requests
import xml.etree.ElementTree as ET
import csv
import time
import random
import cloudscraper

def get_urls_from_product_sitemaps(total_sitemaps=493):
    scraper = cloudscraper.create_scraper()
    all_urls = []
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
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
            print(f"[✓] Sitemap {i}: {len(urls)} URLs (total so far: {len(all_urls)})")
            
            # Be polite - small delay between sitemaps
            time.sleep(random.uniform(0.5, 1.5))
            
        except Exception as e:
            print(f"[!] Error on sitemap {i}: {e}")
            continue
    
    # Save all URLs to CSV
    with open("product_urls.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url"])
        for url in all_urls:
            writer.writerow([url])
    
    print(f"\n[✅] Done! Collected {len(all_urls)} product URLs → saved to product_urls.csv")
    return all_urls

get_urls_from_product_sitemaps(493)
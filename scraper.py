import requests
import csv
import os
from datetime import datetime
import random

PRICES_FILE = "prices.csv"

PRODUCTS = [
    {"product_id": "milk_store", "url": "https://fakestoreapi.com/products/1"},
    {"product_id": "bread_store", "url": "https://fakestoreapi.com/products/2"},
    {"product_id": "eggs_store", "url": "https://fakestoreapi.com/products/3"},
]

def scrape():
    file_exists = os.path.exists(PRICES_FILE)

    with open(PRICES_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(["scraped_at", "product_id", "price", "source_url"])

        for p in PRODUCTS:
            print(f"Fetching {p['url']}")
            try:
                resp = requests.get(p["url"], timeout=20)
                resp.raise_for_status()
                data = resp.json()

                base_price = float(data["price"])
                price = round(base_price * (1 + random.uniform(-0.08, 0.08)), 2)

                writer.writerow([
                    datetime.utcnow().isoformat(),
                    p["product_id"],
                    price,
                    p["url"]
                ])

            except Exception as e:
                print(f"Skipping {p['url']} due to error")

    print("Daily scraping complete.")

if __name__ == "__main__":
    scrape()

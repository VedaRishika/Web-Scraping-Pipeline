import requests
from bs4 import BeautifulSoup
import json, sqlite3, csv, os
from datetime import datetime

DB_PATH = "prices.db"
CSV_PATH = "prices.csv"
PRODUCTS_FILE = "products.json"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        price TEXT,
        scraped_at TEXT,
        source_url TEXT
    )
    """)
    conn.commit()
    return conn

def scrape():
    with open(PRODUCTS_FILE) as f:
        products = json.load(f)

    conn = init_db()
    cur = conn.cursor()

    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(["product_id", "price", "scraped_at", "url"])

        for p in products:
            print(f"Fetching {p['url']}")
            r = requests.get(p["url"], timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")

            el = soup.select_one(p["selector"])
            price = el.get_text(strip=True) if el else "N/A"

            ts = datetime.utcnow().isoformat()

            cur.execute(
                """
                INSERT INTO prices (product_id, price, scraped_at, source_url)
                VALUES (?, ?, ?, ?)
                """,
                (p["product_id"], price, ts, p["url"])
            )

            writer.writerow([p["product_id"], price, ts, p["url"]])

    conn.commit()
    conn.close()
    print("Scraping complete.")

if __name__ == "__main__":
    scrape()

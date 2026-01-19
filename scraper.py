import requests, json, sqlite3, csv, os, random
from datetime import datetime, date

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
        price REAL,
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
    today = date.today().isoformat()

    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(["product_id", "price", "scraped_at", "url"])

        for p in products:
            print(f"Fetching {p['url']}")
            headers = {"User-Agent": "Mozilla/5.0"}
            try:
                resp = requests.get(p["url"], headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                print(f"Skipping {p['url']} (invalid API response)")
                continue

            base_price = float(data["price"])
            price = base_price * (1 + random.uniform(-0.05, 0.05))  # simulate market movement

            # Enforce one record per product per day
            cur.execute(
                "SELECT 1 FROM prices WHERE product_id=? AND date(scraped_at)=?",
                (p["product_id"], today),
            )
            if cur.fetchone():
                continue

            ts = datetime.utcnow().isoformat()
            cur.execute("""
                INSERT INTO prices (product_id, price, scraped_at, source_url)
                VALUES (?, ?, ?, ?)
            """, (p["product_id"], price, ts, p["url"]))
            writer.writerow([p["product_id"], price, ts, p["url"]])

    conn.commit()
    conn.close()
    print("Daily scraping complete.")

if __name__ == "__main__":
    scrape()

import pandas as pd
import sqlite3

DB_PATH = "prices.db"

def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM prices", conn)
    conn.close()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    return df

def build_historical(df):
    return df.sort_values(["product_id", "scraped_at"])

def fill_missing_days(df):
    out = []
    for pid, g in df.groupby("product_id"):
        if len(g) < 2:
            continue
        g = g.set_index("scraped_at").asfreq("D")
        g["product_id"] = pid
        g["price"] = g["price"].interpolate()
        out.append(g.reset_index())
    return pd.concat(out) if out else pd.DataFrame()

def compute_metrics(df):
    rows = []
    for pid, g in df.groupby("product_id"):
        g = g.sort_values("scraped_at")
        g["returns"] = g["price"].pct_change()
        rows.append({
            "product_id": pid,
            "avg_inflation_%": round(g["returns"].mean() * 100, 3),
            "volatility_%": round(g["returns"].std() * 100, 3)
        })
    return pd.DataFrame(rows)

def detect_spikes(df, threshold=10):
    out = []
    for pid, g in df.groupby("product_id"):
        g = g.sort_values("scraped_at")
        g["daily_change_%"] = g["price"].pct_change() * 100
        out.append(g[abs(g["daily_change_%"]) > threshold])
    return pd.concat(out) if out else pd.DataFrame()

def detect_anomalies(df, z=3):
    out = []
    for pid, g in df.groupby("product_id"):
        mean, std = g["price"].mean(), g["price"].std()
        if std == 0:
            continue
        g["z_score"] = (g["price"] - mean) / std
        out.append(g[abs(g["z_score"]) > z])
    return pd.concat(out) if out else pd.DataFrame()

def main():
    df = load_data()
    if df.empty:
        print("No valid data yet.")
        return

    hist = fill_missing_days(build_historical(df))
    if hist.empty:
        print("Not enough history to compute metrics yet.")
        return

    compute_metrics(hist).to_csv("price_metrics.csv", index=False)
    detect_spikes(hist).to_csv("price_spikes.csv", index=False)
    detect_anomalies(hist).to_csv("price_anomalies.csv", index=False)
    hist.to_csv("historical_prices.csv", index=False)

    print("Metrics pipeline complete.")

if __name__ == "__main__":
    main()

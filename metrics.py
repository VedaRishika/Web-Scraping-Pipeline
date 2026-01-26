import pandas as pd
import os
from datetime import datetime

TODAY_FILE = "prices.csv"
HIST_FILE = "historical_prices.csv"
METRICS_FILE = "price_metrics.csv"
ANOMALY_FILE = "price_anomalies.csv"


def load_today_prices():
    if not os.path.exists(TODAY_FILE):
        return pd.DataFrame()

    df = pd.read_csv(TODAY_FILE)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["date"] = df["scraped_at"].dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    return df[["date", "product_id", "price"]]


def update_historical(today_df):
    if today_df.empty:
        return pd.DataFrame()

    if os.path.exists(HIST_FILE):
        hist = pd.read_csv(HIST_FILE)
        hist["date"] = pd.to_datetime(hist["date"]).dt.date
        combined = pd.concat([hist, today_df], ignore_index=True)
    else:
        combined = today_df.copy()

    combined.to_csv(HIST_FILE, index=False)
    return combined


def compute_metrics(hist):
    metrics = []

    for pid, g in hist.groupby("product_id"):
        g = g.sort_values("date")
        returns = g["price"].pct_change()

        inflation = returns.mean()
        volatility = returns.std()

        metrics.append({
            "product_id": pid,
            "inflation_rate": round(inflation, 4),
            "volatility": round(volatility, 4)
        })

    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(METRICS_FILE, index=False)
    return metrics_df


def detect_anomalies(hist, threshold=1.5):
    anomalies = []

    for pid, g in hist.groupby("product_id"):
        g = g.sort_values("date")
        returns = g["price"].pct_change()
        mean = returns.mean()
        std = returns.std()

        if std == 0 or pd.isna(std):
            continue

        z_scores = (returns - mean) / std

        for i in range(len(z_scores)):
            if abs(z_scores.iloc[i]) > threshold:
                anomalies.append({
                    "product_id": pid,
                    "date": g.iloc[i]["date"],
                    "price": g.iloc[i]["price"],
                    "z_score": round(z_scores.iloc[i], 2)
                })

    anomalies_df = pd.DataFrame(anomalies)
    anomalies_df.to_csv(ANOMALY_FILE, index=False)
    return anomalies_df


def main():
    today_df = load_today_prices()
    if today_df.empty:
        print("No valid data today.")
        return

    hist = update_historical(today_df)

    if len(hist) < 3:
        print("Not enough history yet for metrics.")
        return

    compute_metrics(hist)
    detect_anomalies(hist)

    print("Historical table, metrics, and anomalies updated.")


if __name__ == "__main__":
    main()

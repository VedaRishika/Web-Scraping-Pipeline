import pandas as pd
import os

HIST_FILE = "historical_prices.csv"
METRICS_FILE = "price_metrics.csv"
ANOMALY_FILE = "price_anomalies.csv"


def load_history():
    if not os.path.exists(HIST_FILE):
        print("No historical data found.")
        return pd.DataFrame()

    df = pd.read_csv(HIST_FILE)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df["date"] = df["scraped_at"].dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])

    return df[["date", "product_id", "price"]]


def compute_metrics(df):
    metrics = []

    for pid, g in df.groupby("product_id"):
        g = g.sort_values("date")

        returns = g["price"].pct_change()

        if returns.dropna().empty:
            continue

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


def detect_anomalies(df, threshold=1.5):
    anomalies = []

    for pid, g in df.groupby("product_id"):
        g = g.sort_values("date")

        returns = g["price"].pct_change()
        mean = returns.mean()
        std = returns.std()

        if pd.isna(std) or std == 0:
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
    df = load_history()

    if df.empty or len(df) < 3:
        print("Not enough historical data yet.")
        return

    compute_metrics(df)
    detect_anomalies(df)

    print("Metrics and anomalies updated successfully.")


if __name__ == "__main__":
    main()

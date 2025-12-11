from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_data"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and KEY must be set in .env")

# Directories
BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Output files
SUMMARY_CSV = PROCESSED_DIR / "summary_metrics.csv"
CITY_RISK_CSV = PROCESSED_DIR / "city_risk_distribution.csv"
TRENDS_CSV = PROCESSED_DIR / "pollution_trends.csv"
PM25_HIST = PROCESSED_DIR / "pm25_histogram.png"
RISK_BAR = PROCESSED_DIR / "risk_flags_per_city.png"
PM25_LINE = PROCESSED_DIR / "hourly_pm25_trends.png"
SEVERITY_SCATTER = PROCESSED_DIR / "severity_vs_pm25.png"

def fetch_supabase_data() -> pd.DataFrame:
    """Fetch all data from Supabase table."""
    res = supabase.table(TABLE_NAME).select("*").execute()
    data = res.data
    df = pd.DataFrame(data)
    # Convert time to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df

def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Compute requested KPI metrics."""
    summary = {}
    # City with highest average PM2.5
    pm25_avg = df.groupby("city")["pm2_5"].mean()
    summary["city_highest_pm2_5"] = pm25_avg.idxmax()
    summary["avg_pm2_5"] = pm25_avg.max()

    # City with highest severity score
    severity_avg = df.groupby("city")["severity_score"].mean()
    summary["city_highest_severity"] = severity_avg.idxmax()
    summary["avg_severity_score"] = severity_avg.max()

    # Percentage of High/Moderate/Low risk hours
    risk_counts = df["risk_flag"].value_counts(normalize=True) * 100
    summary.update({
        "high_risk_pct": risk_counts.get("High Risk", 0),
        "moderate_risk_pct": risk_counts.get("Moderate Risk", 0),
        "low_risk_pct": risk_counts.get("Low Risk", 0)
    })

    # Hour of day with worst AQI (highest pm2_5)
    hourly_pm25 = df.groupby(df["time"].dt.hour)["pm2_5"].mean()
    summary["worst_aqi_hour"] = int(hourly_pm25.idxmax())
    summary["worst_aqi_pm2_5"] = hourly_pm25.max()

    return pd.DataFrame([summary])

def city_risk_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Compute count and percentage of each risk_flag per city."""
    city_counts = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
    city_totals = df.groupby("city").size().reset_index(name="total")
    city_risk = pd.merge(city_counts, city_totals, on="city")
    city_risk["percentage"] = (city_risk["count"] / city_risk["total"] * 100).round(2)
    return city_risk

def pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Extract time → pm2_5, pm10, ozone per city."""
    trends = df[["city", "time", "pm2_5", "pm10", "ozone"]].copy()
    return trends

def create_plots(df: pd.DataFrame, city_risk_df: pd.DataFrame):
    """Generate required visualizations."""

    # Histogram of PM2.5
    plt.figure(figsize=(8,6))
    sns.histplot(df["pm2_5"].dropna(), bins=30, kde=True, color="orange")
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.savefig(PM25_HIST)
    plt.close()

    # Bar chart: risk flags per city
    plt.figure(figsize=(8,6))
    sns.countplot(data=df, x="city", hue="risk_flag")
    plt.title("Risk Flags per City")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.legend(title="Risk Flag")
    plt.savefig(RISK_BAR)
    plt.close()

    # Line chart: hourly PM2.5 trends per city
    plt.figure(figsize=(10,6))
    df_grouped = df.groupby(["city", df["time"].dt.hour])["pm2_5"].mean().reset_index()
    sns.lineplot(data=df_grouped, x="time", y="pm2_5", hue="city", marker="o")
    plt.title("Hourly PM2.5 Trends per City")
    plt.xlabel("Hour of Day")
    plt.ylabel("PM2.5")
    plt.savefig(PM25_LINE)
    plt.close()

    # Scatter: severity_score vs pm2_5
    plt.figure(figsize=(8,6))
    sns.scatterplot(data=df, x="pm2_5", y="severity_score", hue="city")
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.savefig(SEVERITY_SCATTER)
    plt.close()

def main():
    print("Fetching air quality data from Supabase...")
    df = fetch_supabase_data()
    if df.empty:
        print("⚠️ No data found in Supabase table.")
        return

    print("Computing KPIs...")
    summary_df = compute_kpis(df)
    summary_df.to_csv(SUMMARY_CSV, index=False)
    print(f"✅ Summary metrics saved: {SUMMARY_CSV}")

    print("Computing city risk distribution...")
    city_risk_df = city_risk_distribution(df)
    city_risk_df.to_csv(CITY_RISK_CSV, index=False)
    print(f"✅ City risk distribution saved: {CITY_RISK_CSV}")

    print("Generating pollution trends...")
    trends_df = pollution_trends(df)
    trends_df.to_csv(TRENDS_CSV, index=False)
    print(f"✅ Pollution trends saved: {TRENDS_CSV}")

    print("Creating visualizations...")
    create_plots(df, city_risk_df)
    print("✅ Visualizations saved in data/processed/")

if __name__ == "__main__":
    main()

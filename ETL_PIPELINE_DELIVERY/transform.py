from __future__ import annotations
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Directories
RAW_DIR = Path(__file__).resolve().parents[0] / "data" / "raw"
STAGED_DIR = Path(__file__).resolve().parents[0] / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"

# Required columns
REQUIRED_COLUMNS = [
    "city",
    "time",
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "uv_index"
]

def load_raw_json(file_path: Path) -> pd.DataFrame:
    """Load a single raw JSON file and flatten into a DataFrame."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    city_name = data.get("city") or data.get("meta", {}).get("city") or file_path.stem.split("_")[0]


    hourly = data.get("hourly", {})
    
    if not hourly:
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    df["city"] = city_name

    # Convert time to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    
    # Ensure all required pollutant columns exist
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Keep only required columns
    df = df[REQUIRED_COLUMNS]
    return df

def compute_aqi(pm2_5: float) -> str:
    """Compute AQI category based on PM2.5."""
    if pd.isna(pm2_5):
        return pd.NA
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

def compute_severity(row: pd.Series) -> float:
    """Compute pollution severity score using weighted pollutants."""
    weights = {
        "pm2_5": 5,
        "pm10": 3,
        "nitrogen_dioxide": 4,
        "sulphur_dioxide": 4,
        "carbon_monoxide": 2,
        "ozone": 3
    }
    score = 0
    for pollutant, weight in weights.items():
        value = row.get(pollutant)
        if pd.notna(value):
            score += value * weight
    return score

def classify_risk(severity: float) -> str:
    if pd.isna(severity):
        return pd.NA
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"

def transform_all() -> pd.DataFrame:
    """Load all raw files, transform, and return combined DataFrame."""
    all_files = list(RAW_DIR.glob("*_raw_*.json"))
    dfs = []
    for f in all_files:
        df = load_raw_json(f)
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    combined = pd.concat(dfs, ignore_index=True)

    # Convert numeric columns
    numeric_cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]
    for col in numeric_cols:
        combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # Drop rows where all pollutants are missing
    combined = combined.dropna(subset=numeric_cols, how="all")

    # Feature Engineering
    combined["AQI"] = combined["pm2_5"].apply(compute_aqi)
    combined["severity"] = combined.apply(compute_severity, axis=1)
    combined["risk"] = combined["severity"].apply(classify_risk)
    combined["hour"] = combined["time"].dt.hour

    return combined

if __name__ == "__main__":
    print("Starting transformation of raw air quality data...")
    df_transformed = transform_all()
    if not df_transformed.empty:
        df_transformed.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Transformed data saved to {OUTPUT_FILE}")
        print(f"Total records: {len(df_transformed)}")
    else:
        print("⚠️ No data to transform.")

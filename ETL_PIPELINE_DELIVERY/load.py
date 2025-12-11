from __future__ import annotations
import os
import pandas as pd
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_data"  # Updated table name
BATCH_SIZE = 200
MAX_RETRIES = 2

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and KEY must be set in .env")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Paths
BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"
TRANSFORMED_CSV = STAGED_DIR / "air_quality_transformed.csv"

def prepare_record(row: pd.Series) -> dict:
    """Convert a row to a dict suitable for Supabase insert."""
    record = row.to_dict()
    for k, v in record.items():
        if pd.isna(v):
            record[k] = None  # NaN → NULL
        elif isinstance(v, pd.Timestamp):
            record[k] = v.isoformat()  # datetime → ISO string
        elif isinstance(v, float):
            # Supabase JSON cannot have NaN or inf
            if pd.isna(v) or v in [float("inf"), float("-inf")]:
                record[k] = None
    return record

def load_to_supabase(csv_path: Path):
    df = pd.read_csv(csv_path)
    total_records = len(df)
    print(f"🚀 Loading {total_records} records into Supabase table '{TABLE_NAME}'...")
    inserted_count = 0
    for start in range(0, total_records, BATCH_SIZE):
        batch_df = df.iloc[start:start + BATCH_SIZE]
        # Rename columns from CSV to match your table+------------------
        batch_df = batch_df.rename(columns={
            "AQI": "aqi_category",
            "severity": "severity_score",
            "risk": "risk_flag"
        })
        batch_records = [prepare_record(row) for _, row in batch_df.iterrows()]
        attempt = 0
        while attempt <= MAX_RETRIES:
            try:
                supabase.table(TABLE_NAME).insert(batch_records).execute()
                inserted_count += len(batch_records)
                break  # success, exit retry loop+
            except Exception as e:
                attempt += 1
                print(f"⚠️ Batch insert failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(2 ** attempt)  # exponential backoff
        else:
            print(f"❌ Failed to insert batch starting at row {start}")

    print(f"✅ Finished loading. Total records inserted: {inserted_count}/{total_records}")

if __name__ == "__main__":
    if not TRANSFORMED_CSV.exists():
        print(f"⚠️ Transformed CSV not found: {TRANSFORMED_CSV}")
    else:
        load_to_supabase(TRANSFORMED_CSV)
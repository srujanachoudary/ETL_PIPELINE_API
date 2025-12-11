from extract import extract_delivery_data
from transform import transform_delivery_data
from load import load_to_supabase, save_processed_csv

def run_etl():
    raw_file = extract_delivery_data(
        shipment_id="SHP12345",
        source_city="Hyderabad",
        destination_city="Bangalore",
        dispatch_time="2025-12-10 10:00:00",
        expected_delivery_time="2025-12-11 18:00:00",
        actual_delivery_time="2025-12-11 17:45:00",
        package_weight="2.5kg",
        delivery_agent_id="AGT56789"
    )

    df = transform_delivery_data(raw_file)
    save_processed_csv(df)
    load_to_supabase(df)

if __name__ == "__main__":
    run_etl()

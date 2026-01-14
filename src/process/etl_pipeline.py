import pandas as pd
import io
import os
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

def get_service_client():
    if not STORAGE_ACCOUNT_NAME:
        raise ValueError("Environment variable AZURE_STORAGE_ACCOUNT_NAME is not set.")
    
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", 
        credential=credential
    )
    return service_client

def read_csv_from_datalake(file_system, file_path):
    client = get_service_client().get_file_system_client(file_system).get_file_client(file_path)
    download = client.download_file()
    downloaded_bytes = download.readall()
    return pd.read_csv(io.BytesIO(downloaded_bytes))

def write_parquet_to_datalake(df, file_system, file_path):
    client = get_service_client().get_file_system_client(file_system).get_file_client(file_path)
    # Convert to parquet bytes
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    # Upload
    client.upload_data(parquet_buffer.getvalue(), overwrite=True)

def run_etl():
    """
    1. Read 'bronze/ridership_raw.csv' from ADLS.
    2. Transform.
    3. Write 'silver/ridership_clean.parquet' to ADLS.
    4. Aggregate.
    5. Write 'gold/ridership_features.parquet' to ADLS.
    """
    print(f"[{datetime.now()}] Starting ETL Pipeline (Source: Azure ADLS)...")
    
    if not STORAGE_ACCOUNT_NAME:
        print("ERROR: AZURE_STORAGE_ACCOUNT_NAME env var missing.")
        return

    # --- Step 1: Bronze to Silver ---
    print(f"[{datetime.now()}] Reading Bronze data from Azure...")
    try:
        df_bronze = read_csv_from_datalake("bronze", "ridership_raw.csv")
    except Exception as e:
        print(f"ERROR reading bronze data: {e}")
        return

    # Basic cleaning
    df_bronze.columns = [c.lower().replace(' ', '_') for c in df_bronze.columns]
    
    cols_to_keep = ['transit_timestamp', 'station_complex', 'borough', 'ridership', 'latitude', 'longitude']
    available_cols = [c for c in cols_to_keep if c in df_bronze.columns]
    df_silver = df_bronze[available_cols].copy()

    if 'transit_timestamp' in df_silver.columns:
        df_silver['transit_timestamp'] = pd.to_datetime(df_silver['transit_timestamp'])
        df_silver['date'] = df_silver['transit_timestamp'].dt.date
        df_silver['hour'] = df_silver['transit_timestamp'].dt.hour
        df_silver['day_of_week'] = df_silver['transit_timestamp'].dt.dayofweek
    
    print(f"[{datetime.now()}] Writing Silver data to Azure...")
    write_parquet_to_datalake(df_silver, "silver", "ridership_clean.parquet")

    # --- Step 2: Silver to Gold ---
    print(f"[{datetime.now()}] Creating Gold features...")
    df_gold = df_silver.groupby(['station_complex', 'borough', 'latitude', 'longitude', 'hour', 'day_of_week'])['ridership'].mean().reset_index()
    df_gold.rename(columns={'ridership': 'avg_ridership'}, inplace=True)
    
    print(f"[{datetime.now()}] Writing Gold data to Azure...")
    write_parquet_to_datalake(df_gold, "gold", "ridership_features.parquet")
    print(f"[{datetime.now()}] ETL Complete.")

if __name__ == "__main__":
    run_etl()

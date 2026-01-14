import requests
import os
import pandas as pd
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MTA_DATA_URL = "https://data.ny.gov/api/views/wujg-7c2s/rows.csv?accessType=DOWNLOAD"
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

def fetch_and_upload_data():
    """
    Downloads MTA data and uploads to Azure Data Lake 'bronze' container.
    """
    print(f"[{datetime.now()}] Starting Ingestion to Azure Data Lake...")
    
    if not STORAGE_ACCOUNT_NAME:
        print("ERROR: AZURE_STORAGE_ACCOUNT_NAME env var missing. Run 'export AZURE_STORAGE_ACCOUNT_NAME=...'")
        return

    try:
        service_client = get_service_client()
        file_system_client = service_client.get_file_system_client("bronze")
        
        # Define target file path in Data Lake
        file_client = file_system_client.get_file_client("ridership_raw.csv")
        
        print(f"[{datetime.now()}] Downloading data stream from MTA...")
        # Stream download keeping memory usage low for large files
        response = requests.get(MTA_DATA_URL, stream=True)
        response.raise_for_status()
        
        print(f"[{datetime.now()}] Uploading to Azure ADLS (bronze/ridership_raw.csv)...")
        # Upload data
        chunk_size = 10 * 1024 * 1024 # 10MB
        total_size = 0
        max_size = 500 * 1024 * 1024 # 500MB limit for better accuracy
        
        data = b""
        for chunk in response.iter_content(chunk_size=chunk_size):
            data += chunk
            total_size += len(chunk)
            print(f"Downloaded {total_size / (1024*1024):.1f} MB...")
            if total_size > max_size:
                break
                
        file_client.upload_data(data, overwrite=True)
        
        print(f"[{datetime.now()}] Upload Complete.")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    fetch_and_upload_data()

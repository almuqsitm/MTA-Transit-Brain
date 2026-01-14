import pandas as pd
import io
import os
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error
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

def read_parquet_from_datalake(file_system, file_path):
    client = get_service_client().get_file_system_client(file_system).get_file_client(file_path)
    download = client.download_file()
    downloaded_bytes = download.readall()
    return pd.read_parquet(io.BytesIO(downloaded_bytes))

def train_model():
    print(f"[{datetime.now()}] Starting Model Training (Source: Azure Gold)...")
    
    if not STORAGE_ACCOUNT_NAME:
        print("ERROR: AZURE_STORAGE_ACCOUNT_NAME env var missing.")
        return

    # Load Data
    try:
        df = read_parquet_from_datalake("gold", "ridership_features.parquet")
    except Exception as e:
        print(f"ERROR reading Gold data: {e}")
        return

    # Preprocessing
    le = LabelEncoder()
    df['station_id_encoded'] = le.fit_transform(df['station_complex'])
    
    X = df[['station_id_encoded', 'hour', 'day_of_week', 'latitude', 'longitude']]
    y = df['avg_ridership']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print(f"[{datetime.now()}] Training Random Forest Regressor...")
    model = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    print(f"[{datetime.now()}] Model Training Completed. MAE: {mae:.2f}")
    
    # Save Model Locally (for App usage)
    # We also upload these to a container? For now local is fine for the App.
    os.makedirs("src/models", exist_ok=True)
    joblib.dump(model, "src/models/ridership_model.pkl")
    joblib.dump(le, "src/models/station_encoder.pkl")
    print(f"[{datetime.now()}] Model saved locally.")

if __name__ == "__main__":
    train_model()

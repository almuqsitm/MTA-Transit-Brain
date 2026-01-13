import streamlit as st
import pandas as pd
import joblib
import pydeck as pdk
import io
import os
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Configuration
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
MODEL_PATH = os.path.join("src", "models", "ridership_model.pkl")
ENCODER_PATH = os.path.join("src", "models", "station_encoder.pkl")

# Page Config
st.set_page_config(page_title="MTA Azure Transit Brain", layout="wide")

st.title("🚇 MTA Intelligent Transit Analytics Platform")
st.markdown("**Enterprise Architecture: Azure Data Lake Gen2 -> Azure ML -> Streamlit**")

def get_service_client():
    if not STORAGE_ACCOUNT_NAME:
        return None
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", 
        credential=credential
    )

@st.cache_data(ttl=3600)
def load_gold_data():
    """Download Gold data from Azure to get Station Metadata"""
    if not STORAGE_ACCOUNT_NAME:
        return None
    
    try:
        client = get_service_client().get_file_system_client("gold").get_file_client("ridership_features.parquet")
        download = client.download_file()
        return pd.read_parquet(io.BytesIO(download.readall()))
    except Exception as e:
        st.error(f"Failed to load data from Azure: {e}")
        return None

@st.cache_resource
def load_model():
    try:
        model = joblib.load(MODEL_PATH)
        le = joblib.load(ENCODER_PATH)
        return model, le
    except FileNotFoundError:
        return None, None

# Main App Logic
if not STORAGE_ACCOUNT_NAME:
    st.error("⚠️ Environment variable `AZURE_STORAGE_ACCOUNT_NAME` is missing.")
    st.info("Please set it to your Azure Storage Account name created via Terraform.")
    st.stop()

with st.spinner("Fetching latest data from Azure Data Lake..."):
    df = load_gold_data()

model, le = load_model()

if df is None:
    st.warning("Could not load data. Ensure `src/process/etl_pipeline.py` has run successfully.")
elif model is None:
    st.warning("Could not load model. Ensure `src/models/forecaster.py` has run successfully.")
else:
    # Sidebar
    st.sidebar.header("Forecasting Parameters")
    stations = df['station_complex'].unique()
    selected_station = st.sidebar.selectbox("Select Station Complex", stations)
    
    forecast_date = st.sidebar.date_input("Forecast Date", datetime.now().date())
    forecast_hour = st.sidebar.slider("Hour of Day", 0, 23, 12)
    
    # Logic
    day_of_week = forecast_date.weekday()
    station_data = df[df['station_complex'] == selected_station].iloc[0]
    lat, lon = station_data['latitude'], station_data['longitude']
    
    try:
        # Encode station and predict
        station_encoded = le.transform([selected_station])[0]
        
        input_data = pd.DataFrame({
            'station_id_encoded': [station_encoded],
            'hour': [forecast_hour],
            'day_of_week': [day_of_week],
            'latitude': [lat],
            'longitude': [lon]
        })
        
        prediction = model.predict(input_data)[0]
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Predicted Ridership", f"{int(prediction):,}", delta="Live from Azure ML")
        with col2:
            crowd_level = "High" if prediction > 2000 else "Medium" if prediction > 500 else "Low"
            color = "red" if crowd_level == "High" else "orange" if crowd_level == "Medium" else "green"
            st.markdown(f"**Crowd Level**: :{color}[{crowd_level}]")
        with col3:
            st.metric("Data Source", "Azure ADLS Gen2 (Gold)", "Verified")

        # Map & Chart
        st.subheader(f"📍 Station: {selected_station}")
        st.pydeck_chart(pdk.Deck(
            map_style='mapbox://styles/mapbox/light-v9',
            initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=14, pitch=50),
            layers=[pdk.Layer('ScatterplotLayer', data=pd.DataFrame({'lat': [lat], 'lon': [lon]}), get_position='[lon, lat]', get_color='[200, 30, 0, 160]', get_radius=200)],
        ))
        
        st.subheader("📊 24-Hour Forecast")
        hours = list(range(24))
        trend_input = pd.DataFrame({
            'station_id_encoded': [station_encoded]*24, 'hour': hours, 
            'day_of_week': [day_of_week]*24, 'latitude': [lat]*24, 'longitude': [lon]*24
        })
        trend_preds = model.predict(trend_input)
        st.line_chart(pd.DataFrame({'Hour': hours, 'Ridership': trend_preds}).set_index('Hour'))
        
    except Exception as e:
        st.error(f"Prediction Error: {e}")

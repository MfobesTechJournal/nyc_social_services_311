
import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import io

load_dotenv()

# --- CONFIG FROM SECRETS (Streamlit Cloud) ---
AZURE_CONN_STR = st.secrets["AZURE_CONNECTION_STRING"]
AZURE_CONTAINER = st.secrets.get("AZURE_CONTAINER_NAME", "data")
AZURE_BLOB = "fact_311_export.csv"  # Your exported file

@st.cache_data(ttl=600)  # Refresh every 10 mins
def load_data_from_azure():
    try:
        blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        blob_client = blob_service.get_blob_client(container=AZURE_CONTAINER, blob=AZURE_BLOB)
        
        # Download and read CSV
        download = blob_client.download_blob()
        csv_bytes = download.readall()
        df = pd.read_csv(io.BytesIO(csv_bytes))
        
        df['created_date'] = pd.to_datetime(df['created_date'])
        return df
    except Exception as e:
        st.error(f"Failed to load data from Azure: {e}")
        return pd.DataFrame()

# --- UI ---
st.set_page_config(page_title="NYC 311 Dashboard", layout="wide")
st.title("NYC 311 Social Services Dashboard")
st.markdown("**Live from Azure Blob → `data/fact_311_export.csv`**")

df = load_data_from_azure()

if df.empty:
    st.stop()

# Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Complaints", len(df))
col2.metric("Unique Agencies", df['agency'].nunique())
col3.metric("Date Range", f"{df['created_date'].min().date()} → {df['created_date'].max().date()}")

# Charts
st.subheader("Top 5 Complaint Types")
top5 = df['complaint_type'].value_counts().head(5)
st.bar_chart(top5)

st.subheader("Complaints by Borough")
borough_counts = df['borough'].value_counts()
fig = borough_counts.plot.pie(autopct='%1.1f%%', figsize=(6,6)).figure
st.pyplot(fig)

# Filterable Table
st.subheader("All Complaints (Filterable)")
borough_filter = st.multiselect("Filter by Borough", options=sorted(df['borough'].unique()), default=[])
display_df = df[df['borough'].isin(borough_filter)] if borough_filter else df
st.dataframe(display_df[['created_date', 'complaint_type', 'agency', 'borough']].head(100), use_container_width=True)

st.caption("Built with Streamlit + Azure Blob | Team NYC 311")


import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONNECT_STR = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = "data"
BLOB_NAME = "fact_311_clean.parquet"

@st.cache_data
def load_data():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        
        with open("temp_parquet.parquet", "wb") as f:
            f.write(blob_client.download_blob().readall())
        
        df = pd.read_parquet("temp_parquet.parquet")
        os.remove("temp_parquet.parquet")  # Clean up
        logger.info("Data loaded from Azure!")
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


st.set_page_config(page_title="NYC 311 Dashboard", layout="wide")
st.title("NYC 311 Social Services Dashboard")
st.markdown("**Live data from Azure Blob Storage**")

df = load_data()

if df.empty:
    st.stop()


col1, col2, col3 = st.columns(3)
col1.metric("Total Complaints", len(df))
col2.metric("Unique Agencies", df['agency'].nunique())
col3.metric("Date Range", f"{df['created_date'].min().date()} → {df['created_date'].max().date()}")


st.subheader("Top 5 Complaint Types")
top5 = df['complaint_type'].value_counts().head(5)
st.bar_chart(top5)

st.subheader("Complaints by Borough")
borough_counts = df['borough'].value_counts()
st.pyplot(borough_counts.plot.pie(autopct='%1.1f%%', figsize=(6,6)).figure)


st.subheader("All Complaints (Filterable)")
borough_filter = st.multiselect("Filter by Borough", options=sorted(df['borough'].unique()), default=[])
if borough_filter:
    df = df[df['borough'].isin(borough_filter)]

st.dataframe(df[['created_date', 'complaint_type', 'agency', 'borough']].head(100), use_container_width=True)

# === FOOTER ===
st.caption("Built with Streamlit + Azure Blob | Team NYC 311")

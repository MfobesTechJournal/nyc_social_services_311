# dashboard/app.py
import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIG ---
AZURE_CONN_STR = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "data")
AZURE_BLOB = "fact_311_clean.parquet"          # source in Azure
DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

# --- HELPERS ---
def load_azure_to_sql():
    """One-click button to pull parquet from Azure → PostgreSQL (run once)."""
    if st.sidebar.button("Load Data from Azure to SQL (Run Once)"):
        with st.spinner("Downloading from Azure..."):
            try:
                blob_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
                client = blob_client.get_blob_client(container=AZURE_CONTAINER, blob=AZURE_BLOB)
                with open("temp.parquet", "wb") as f:
                    f.write(client.download_blob().readall())
                st.success("Downloaded from Azure")
            except Exception as e:
                st.error(f"Azure error: {e}")
                return

        with st.spinner("Loading into PostgreSQL..."):
            try:
                df = pd.read_parquet("temp.parquet")
                df['created_date'] = pd.to_datetime(df['created_date'])
                engine = create_engine(DB_URL)
                df.to_sql("fact_311", engine, if_exists='replace', index=False)
                os.remove("temp.parquet")
                st.success(f"Loaded {len(df):,} rows into `fact_311`")
                logger.info("Azure → SQL complete")
            except Exception as e:
                st.error(f"SQL load failed: {e}")

@st.cache_data(ttl=300)  # Refresh every 5 minutes
def load_from_sql():
    """Read live data from PostgreSQL."""
    try:
        engine = create_engine(DB_URL)
        df = pd.read_sql("SELECT * FROM fact_311", engine)
        df['created_date'] = pd.to_datetime(df['created_date'])
        logger.info("Data loaded from PostgreSQL")
        return df
    except Exception as e:
        st.error(f"SQL connection failed: {e}")
        return pd.DataFrame()

# --- UI ---
st.set_page_config(page_title="NYC 311 Dashboard", layout="wide")
st.title("NYC 311 Social Services Dashboard")
st.markdown("**Live pipeline: Azure → PostgreSQL → Streamlit**")

# Sidebar controls
with st.sidebar:
    st.header("Pipeline Control")
    load_azure_to_sql()
    st.divider()
    st.caption("Data auto-refreshes every 5 mins")

# Load data
df = load_from_sql()
if df.empty:
    st.stop()

# Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Complaints", len(df))
col2.metric("Unique Agencies", df['agency'].nunique())
col3.metric("Date Range", f"{df['created_date'].min().date()} → {df['created_date'].max().date()}")

# Top 5 Complaint Types
st.subheader("Top 5 Complaint Types")
top5 = df['complaint_type'].value_counts().head(5)
st.bar_chart(top5)

# Complaints by Borough (Pie)
st.subheader("Complaints by Borough")
borough_counts = df['borough'].value_counts()
fig = borough_counts.plot.pie(autopct='%1.1f%%', figsize=(6,6)).figure
st.pyplot(fig)

# Filterable Table
st.subheader("All Complaints (Filterable)")
borough_filter = st.multiselect(
    "Filter by Borough",
    options=sorted(df['borough'].unique()),
    default=[]
)
display_df = df[df['borough'].isin(borough_filter)] if borough_filter else df
st.dataframe(
    display_df[['created_date', 'complaint_type', 'agency', 'borough']].head(100),
    use_container_width=True
)

st.caption("Built with Streamlit + Azure + PostgreSQL | Team NYC 311")

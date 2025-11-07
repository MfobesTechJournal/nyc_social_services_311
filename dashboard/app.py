
import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io


AZURE_CONN = st.secrets["AZURE_CONNECTION_STRING"]
CONTAINER   = st.secrets.get("AZURE_CONTAINER_NAME", "data")
BLOB        = "fact_311_export.csv"

@st.cache_data(ttl=600)
def load():
    client = BlobServiceClient.from_connection_string(AZURE_CONN)
    blob = client.get_blob_client(container=CONTAINER, blob=BLOB)
    data = blob.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    df['created_date'] = pd.to_datetime(df['created_date'])
    return df

st.set_page_config(page_title="NYC 311 Dashboard", layout="wide")
st.title("NYC 311 Social Services Dashboard")
st.markdown("**Live from Azure Blob → `data/fact_311_export.csv`**")

df = load()
if df.empty:
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Total Complaints", len(df))
c2.metric("Unique Agencies", df['agency'].nunique())
c3.metric("Date Range", f"{df['created_date'].min().date()} → {df['created_date'].max().date()}")

st.subheader("Top 5 Complaint Types")
st.bar_chart(df['complaint_type'].value_counts().head(5))

st.subheader("Complaints by Borough")
fig = df['borough'].value_counts().plot.pie(autopct='%1.1f%%', figsize=(6,6)).figure
st.pyplot(fig)

st.subheader("All Complaints (Filterable)")
borough = st.multiselect("Borough", options=sorted(df['borough'].unique()), default=[])
show = df[df['borough'].isin(borough)] if borough else df
st.dataframe(show[['created_date','complaint_type','agency','borough']].head(100), use_container_width=True)

st.caption("Built with Streamlit + Azure Blob | MySQL backend (local)")

# sql/azure_to_sql.py
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Config
connect_str = os.getenv("AZURE_CONNECTION_STRING")
container = os.getenv("AZURE_CONTAINER_NAME")
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

def download():
    blob = BlobServiceClient.from_connection_string(connect_str)
    client = blob.get_blob_client(container=container, blob="fact_311_clean.parquet")
    with open("results/fact_311_clean.parquet", "wb") as f:
        f.write(client.download_blob().readall())
    print("Downloaded from Azure")

def load():
    df = pd.read_parquet("results/fact_311_clean.parquet")
    df['created_date'] = pd.to_datetime(df['created_date'])
    engine = create_engine(db_url)
    df.to_sql("fact_311", engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows to SQL")

if __name__ == "__main__":
    download()
    load()
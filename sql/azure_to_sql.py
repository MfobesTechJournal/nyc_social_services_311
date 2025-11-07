
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)


AZURE_CONN = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER   = os.getenv("AZURE_CONTAINER_NAME", "data")
BLOB        = "fact_311_clean.parquet"


MYSQL_URL = (
    f"mysql+mysqlconnector://"
    f"{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@"
    f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)

def download():
    client = BlobServiceClient.from_connection_string(AZURE_CONN)
    blob = client.get_blob_client(container=CONTAINER, blob=BLOB)
    path = "results/fact_311_clean.parquet"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(blob.download_blob().readall())
    logging.info("Downloaded from Azure")
    return path

def load_to_mysql(parquet_path):
    df = pd.read_parquet(parquet_path)
    df['created_date'] = pd.to_datetime(df['created_date'])
    engine = create_engine(MYSQL_URL)
    df.to_sql("fact_311", engine, if_exists="replace", index=False)
    logging.info(f"Loaded {len(df):,} rows into MySQL")

if __name__ == "__main__":
    path = download()
    load_to_mysql(path)


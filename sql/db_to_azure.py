
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

MYSQL_URL = (
    f"mysql+mysqlconnector://"
    f"{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@"
    f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)
AZURE_CONN = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER   = os.getenv("AZURE_CONTAINER_NAME", "data")
BLOB_EXPORT = "fact_311_export.csv"

def export():
    engine = create_engine(MYSQL_URL)
    df = pd.read_sql("SELECT * FROM fact_311", engine)
    local = f"results/{BLOB_EXPORT}"
    df.to_csv(local, index=False)
    logging.info(f"Exported {len(df)} rows to {local}")
    return local

def upload(local_file):
    client = BlobServiceClient.from_connection_string(AZURE_CONN)
    blob = client.get_blob_client(container=CONTAINER, blob=BLOB_EXPORT)
    with open(local_file, "rb") as f:
        blob.upload_blob(f, overwrite=True)
    logging.info(f"Uploaded to Azure: {CONTAINER}/{BLOB_EXPORT}")

if __name__ == "__main__":
    upload(export())

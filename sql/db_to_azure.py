# sql/db_to_azure.py
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config from .env
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
connect_str = os.getenv("AZURE_CONNECTION_STRING")
container_name = os.getenv("AZURE_CONTAINER_NAME", "processed")
blob_name = "fact_311_export.csv"  # Output file name in Azure

def export_from_sql(table_name="fact_311"):
    try:
        engine = create_engine(db_url)
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        local_csv = f"results/{blob_name}"
        df.to_csv(local_csv, index=False)
        logger.info(f"Exported {len(df)} rows from SQL to {local_csv}")
        return local_csv
    except Exception as e:
        logger.error(f"SQL export failed: {e}")

def upload_to_azure(local_file):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        with open(local_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        logger.info(f"Uploaded to Azure Blob: {container_name}/{blob_name}")
    except Exception as e:
        logger.error(f"Azure upload failed: {e}")

# MAIN: Export SQL → Upload to Azure
if __name__ == "__main__":
    print("Exporting from PostgreSQL...")
    local_file = export_from_sql()
    
    print("Uploading to Azure...")
    upload_to_azure(local_file)
    
    print("SUCCESS! Database pushed to Azure Blob.")
    print(f"Check: {container_name}/{blob_name}")
# etl/azure_upload.py
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging

# Load secrets from .env
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read from .env
connect_str = os.getenv("AZURE_CONNECTION_STRING")
account_name = os.getenv("AZURE_ACCOUNT_NAME")
container_name = "data"  # We'll upload to 'data' container

def upload_file(local_path, blob_name):
    try:
        # Connect to Azure
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Upload
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        logger.info(f"Uploaded {local_path} → Azure Blob: {blob_name}")
    except Exception as e:
        logger.error(f"Failed: {e}")

# MAIN: Upload both files
if __name__ == "__main__":
    print("Uploading raw CSV...")
    upload_file("data/raw/nyc311.csv", "nyc311_raw.csv")
    
    print("Uploading cleaned data...")
    upload_file("results/fact_311_clean.parquet", "fact_311_clean.parquet")
    
    print("All done! Check Azure Portal.")
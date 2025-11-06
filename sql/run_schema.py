# sql/run_schema.py
import psycopg2
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config (create .env in root)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "nyc311")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def run_schema():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        logger.info("Connected to PostgreSQL")

        # Read and run schema
        with open('sql/schema.sql', 'r') as f:
            schema_sql = f.read()
        cur.execute(schema_sql)
        conn.commit()
        logger.info("Schema executed")

        # Test query
        cur.execute("SELECT agency, agency_name FROM dim_agency")
        result = cur.fetchall()
        logger.info(f"Agencies: {result}")

        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    run_schema()
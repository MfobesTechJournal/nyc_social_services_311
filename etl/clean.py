# etl/clean.py
import pandas as pd
import boto3
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_and_clean():
    logger.info("Extracting raw CSV...")
    df = pd.read_csv("data/raw/nyc311.csv", nrows=2500)

    logger.info(f"Starting with {len(df)} rows")
    df = df[['Unique Key', 'Created Date', 'Complaint Type', 'Agency', 'Borough', 'Incident Zip']].copy()
    df.columns = ['unique_key', 'created_date', 'complaint_type', 'agency', 'borough', 'incident_zip']

    # FIX: Boroughs are in 'Unspecified' or mixed case
    df['borough'] = df['borough'].str.title().str.strip()
    valid_boroughs = ['Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island', 'Unspecified']
    
    invalid_borough = ~df['borough'].isin(valid_boroughs)
    if invalid_borough.sum() > 0:
        logger.warning(f"Dropping {invalid_borough.sum()} invalid boroughs")
        df = df[df['borough'].isin(valid_boroughs)]

    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    df = df.dropna(subset=['unique_key', 'created_date'])

    df['incident_zip'] = pd.to_numeric(df['incident_zip'], errors='coerce')
    invalid_zip = df['incident_zip'].isna() | (df['incident_zip'] < 10000) | (df['incident_zip'] > 99999)
    if invalid_zip.sum() > 0:
        logger.warning(f"Dropping {invalid_zip.sum()} invalid zips")
        df = df[~invalid_zip]

    before = len(df)
    df = df.drop_duplicates(subset=['unique_key'])
    logger.info(f"Removed {before - len(df)} duplicates")

    df.to_parquet("results/fact_311_clean.parquet", index=False)
    logger.info(f"Cleaned: {len(df)} rows → results/fact_311_clean.parquet")

    return df

if __name__ == "__main__":
    extract_and_clean()
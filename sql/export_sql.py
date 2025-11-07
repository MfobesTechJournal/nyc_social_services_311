
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)


df = pd.read_sql("SELECT * FROM fact_311", engine)
df.to_csv("results/sql_export.csv", index=False)

print(f"Exported {len(df)} rows to results/sql_export.csv")

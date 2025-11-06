# sql/run_schema.py
import mysql.connector
from dotenv import load_dotenv
import os
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

cfg = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "nyc311"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "")
}

def run():
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()
    logging.info("Connected to MySQL")

    with open("sql/schema.sql", "r") as f:
        sql = f.read()
    for stmt in sql.split(";"):
        if stmt.strip():
            cur.execute(stmt)
    conn.commit()
    logging.info("Schema applied")

    cur.execute("SELECT * FROM dim_agency")
    logging.info(f"Agencies: {cur.fetchall()}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run()

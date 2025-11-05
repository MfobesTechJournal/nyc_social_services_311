-- sql
DROP TABLE IF EXISTS fact_311;
CREATE TABLE fact_311 AS SELECT * FROM read_csv_auto('data/raw/nyc311.csv');
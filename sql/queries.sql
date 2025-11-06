-- Dimension Table
CREATE TABLE IF NOT EXISTS dim_agency (
    agency VARCHAR(10) PRIMARY KEY,
    agency_name VARCHAR(100)
);
INSERT INTO dim_agency VALUES ('NYPD', 'New York Police Department'), ('DOT', 'Department of Transportation');

-- JOIN + Aggregate
SELECT a.agency_name, COUNT(*) as complaints
FROM fact_311 f
INNER JOIN dim_agency a ON f.agency = a.agency
GROUP BY a.agency_name
ORDER BY complaints DESC;

-- CTE for Hourly Complaints
WITH hourly AS (
    SELECT EXTRACT(HOUR FROM created_date) as hour, COUNT(*) as complaints
    FROM fact_311 GROUP BY hour
)
SELECT hour, complaints
FROM hourly ORDER BY complaints DESC;
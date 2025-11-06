-- Drop tables if exist (for re-runs)
DROP TABLE IF EXISTS fact_311;
DROP TABLE IF EXISTS dim_agency;

-- Dimension: Agencies
CREATE TABLE dim_agency (
    agency VARCHAR(10) PRIMARY KEY,
    agency_name VARCHAR(100)
);

-- Fact: 311 Complaints
CREATE TABLE fact_311 (
    unique_key VARCHAR(50) PRIMARY KEY,
    created_date TIMESTAMP,
    complaint_type VARCHAR(100),
    agency VARCHAR(10),
    borough VARCHAR(50),
    incident_zip VARCHAR(10),
    FOREIGN KEY (agency) REFERENCES dim_agency(agency)
);

-- Insert Sample Data
INSERT INTO dim_agency (agency, agency_name) VALUES
('NYPD', 'New York Police Department'),
('DOT', 'Department of Transportation'),
('HPD', 'Housing Preservation and Development')
ON CONFLICT DO NOTHING;

-- Test CRUD
INSERT INTO fact_311 VALUES ('TEST123', '2024-01-01 10:00:00', 'Noise', 'NYPD', 'Manhattan', '10001')
ON CONFLICT DO NOTHING;

-- Test JOIN
-- SELECT f.unique_key, a.agency_name, f.complaint_type
-- FROM fact_311 f
-- JOIN dim_agency a ON f.agency = a.agency;
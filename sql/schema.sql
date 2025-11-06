-- Drop if exists (safe re-run)
DROP TABLE IF EXISTS fact_311;
DROP TABLE IF EXISTS dim_agency;

-- Dimension table
CREATE TABLE dim_agency (
    agency VARCHAR(10) PRIMARY KEY,
    agency_name VARCHAR(100)
);

-- Fact table
CREATE TABLE fact_311 (
    unique_key VARCHAR(50) PRIMARY KEY,
    created_date DATETIME,
    complaint_type VARCHAR(100),
    agency VARCHAR(10),
    borough VARCHAR(50),
    incident_zip VARCHAR(10),
    FOREIGN KEY (agency) REFERENCES dim_agency(agency)
) ENGINE=InnoDB;

-- Sample data
INSERT IGNORE INTO dim_agency (agency, agency_name) VALUES
('NYPD', 'New York Police Department'),
('DOT',  'Department of Transportation'),
('HPD',  'Housing Preservation and Development');

-- Test row
INSERT IGNORE INTO fact_311 VALUES 
('TEST123', '2024-01-01 10:00:00', 'Noise', 'NYPD', 'Manhattan', '10001');

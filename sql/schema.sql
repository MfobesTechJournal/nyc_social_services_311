-- sql/sche,a.sql
-- NYC 311 Social Services - Service Requests Table

CREATE TABLE IF NOT EXISTS service_requests (
    unique_key          VARCHAR(50)     PRIMARY KEY,
    created_date        TIMESTAMP,
    closed_date         TIMESTAMP,
    agency              VARCHAR(100),
    agency_name         VARCHAR(225),
    complaint_type      VARCHAR(255),
    descriptor          TEXT,
    location_type       VARCHAR(100),
    incident_zip        VARCHAR(10),
    incident_address    VARCHAR(255),
    street_name         VARCHAR(100),
    cross_street_1      VARCHAR(100),
    cross_street_2      VARCHAR(100),
    intersection_street_1 VARCHAR(100),
    intersection_street_2 VARCHAR(100),
    address_type        VARCHAR(50),
    city                VARCHAR(50),
    landmark            VARCHAR(100),
    facility_type       VARCHAR(50),
    status              VARCHAR(50),
    due_date            TIMESTAMP,
    resolution_description TEXT,
    resolution_action_updated_date TIMESTAMP,
    community_board     VARCHAR(50),
    bbl                 VARCHAR(20),
    borough             VARCHAR(50),
    x_coordinate        NUMERIC,
    y_coordinate        NUMERIC,
    open_data_channel_type VARCHAR(50),
    park_facility_name  VARCHAR(255),
    park_borough        VARCHAR(50),
    vehicle_type        VARCHAR(100),
    taxi_company_borough VARCHAR(50),
    taxi_pick_up_location VARCHAR(255),
    bridge_highway_name VARCHAR(255),
    bridge_highway_direction VARCHAR(50),
    road_ramp           VARCHAR(50),
    bridge_highway_segment VARCHAR(255),
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),
    location            TEXT
);

-- Indexes for fast queries
CREATE INDEX idx_requests_borough ON service_requests(borough);
CREATE INDEX idx_requests_complaint ON service_requests(complaint_type);
CREATE INDEX idx_requests_date ON service_requests(created_date);
CREATE INDEX idx_requests_agency ON service_requests(agency);
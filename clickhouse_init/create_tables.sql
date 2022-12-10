-- ANALYTICS TABLES 
CREATE TABLE IF NOT EXISTS vehicle_position_data(
    date Date,
    vehicle_id String,
    updated_at DateTime,
    lat Float64,
    long Float64,
    driven_meters Float64
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date);

CREATE TABLE IF NOT EXISTS vehicle_operation_metadata(
    date Date,
    vehicle_id String,
    operating_period_id String,
    operation_start DateTime,
    operation_finish DateTime
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date);

-- EVENTS TABLES
CREATE TABLE IF NOT EXISTS create_events(
    operating_period_id String,
    created_at DateTime,
    start DateTime,
    finish DateTime,
    date Date,
    organization_id String
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date);

CREATE TABLE IF NOT EXISTS delete_events(
    operating_period_id String,
    deleted_at DateTime,
    start DateTime,
    finish DateTime,
    date Date,
    organization_id String
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date);

CREATE TABLE IF NOT EXISTS deregister_events(
    vehicle_id String,
    deregisted_at DateTime,
    date Date,
    organization_id String
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date);

CREATE TABLE IF NOT EXISTS register_events(
    vehicle_id String,
    registed_at DateTime,
    date Date,
    organization_id String
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date);

CREATE TABLE IF NOT EXISTS update_events(
    vehicle_id String,
    updated_at DateTime,
    lat Float64,
    long Float64,
    date Date,
    organization_id String
) ENGINE MergeTree() PARTITION BY date
ORDER BY
    (date)
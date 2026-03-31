CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS machines (
    mac TEXT PRIMARY KEY,
    device_name TEXT,
    model TEXT,
    site TEXT,
    wash_temp_setpoint INT,
    rinse_temp_setpoint INT,
    dosing_unit_type TEXT,
    energy_kw REAL,
    raw JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dishwasher_data (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    device_id TEXT NOT NULL,
    mac_address_string TEXT,
    beacon_event_id INT,
    mean_wash_tank_temp DOUBLE PRECISION,
    mean_rinse_temp DOUBLE PRECISION,
    mean_conductivity DOUBLE PRECISION,
    setpoint_conductivity DOUBLE PRECISION,
    temp_below_setpoint BOOLEAN DEFAULT FALSE,
    conc_constant_over BOOLEAN DEFAULT FALSE,
    conc_constant_under BOOLEAN DEFAULT FALSE,
    rinse_temp_below_setpoint BOOLEAN DEFAULT FALSE,
    conc_ok BOOLEAN DEFAULT FALSE,
    wash_temp_recovery_failure BOOLEAN DEFAULT FALSE,
    rinse_temp_recovery_failure BOOLEAN DEFAULT FALSE,
    conc_recovery_failure BOOLEAN DEFAULT FALSE,
    wash_temp_repeated_issue BOOLEAN DEFAULT FALSE,
    rinse_temp_repeated_issue BOOLEAN DEFAULT FALSE,
    conc_repeated_issue BOOLEAN DEFAULT FALSE,
    wash_temp_high_variance BOOLEAN DEFAULT FALSE,
    rinse_temp_high_variance BOOLEAN DEFAULT FALSE,
    schema_valid BOOLEAN DEFAULT TRUE,
    en_wash_on INTEGER,
    en_rinse_on INTEGER,
    op_state INTEGER,
    date_time_utc BIGINT,
    event_time_utc_ms BIGINT NOT NULL,
    kafka_logappend_ms BIGINT,
    produced_at_ms BIGINT,
    ingest_at TIMESTAMPTZ,
    persisted_at TIMESTAMPTZ,
    event_latency_seconds DOUBLE PRECISION,
    latency_seconds DOUBLE PRECISION,
    fal_seconds DOUBLE PRECISION,
    stream_features_ready_at TIMESTAMPTZ,
    flags_computed_at TIMESTAMP,
    window_integrity_5min DOUBLE PRECISION,
    field_consistency_score DOUBLE PRECISION,
    missing_rate DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    features_computed_at TIMESTAMPTZ,
    features_ready_ms BIGINT,
    UNIQUE (mac_address_string, beacon_event_id, event_time_utc_ms, kafka_logappend_ms, produced_at_ms)
);

CREATE INDEX IF NOT EXISTS idx_dishwasher_device_datetime
    ON dishwasher_data (device_id, date_time_utc DESC);

CREATE INDEX IF NOT EXISTS idx_dishwasher_event_time_utc_ms ON dishwasher_data(event_time_utc_ms);
CREATE INDEX IF NOT EXISTS idx_dishwasher_kafka_logappend_ms ON dishwasher_data(kafka_logappend_ms);
CREATE INDEX IF NOT EXISTS idx_dishwasher_ingest_at ON dishwasher_data(ingest_at);
CREATE INDEX IF NOT EXISTS idx_dishwasher_persisted_at ON dishwasher_data(persisted_at);
CREATE INDEX IF NOT EXISTS idx_dishwasher_mac ON dishwasher_data(mac_address_string);

CREATE OR REPLACE FUNCTION calculate_event_latency()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.persisted_at IS NULL THEN
        NEW.persisted_at := NOW();
    END IF;
    IF NEW.ingest_at IS NULL THEN
        NEW.ingest_at := NEW.persisted_at;
    END IF;
    IF NEW.kafka_logappend_ms IS NOT NULL AND NEW.persisted_at IS NOT NULL THEN
        NEW.event_latency_seconds := (EXTRACT(EPOCH FROM NEW.persisted_at) * 1000.0 - NEW.kafka_logappend_ms) / 1000.0;
    END IF;
    NEW.latency_seconds := NEW.event_latency_seconds;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER latency_trigger
    BEFORE INSERT ON dishwasher_data
    FOR EACH ROW
    EXECUTE FUNCTION calculate_event_latency();

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO archuser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO archuser;
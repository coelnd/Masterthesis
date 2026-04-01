CREATE TABLE IF NOT EXISTS machines (
  mac TEXT PRIMARY KEY,
  device_name TEXT,
  model TEXT,
  site TEXT,
  wash_temp_setpoint INT,
  rinse_temp_setpoint INT,
  dosing_unit_type TEXT,
  energy_kw REAL,
  raw JSONB
);

CREATE TABLE IF NOT EXISTS dishwasher_data (
  id SERIAL PRIMARY KEY,
  event_id TEXT UNIQUE, 
  device_id TEXT,
  mac_address_string TEXT,
  beacon_event_id INTEGER,
  mean_conductivity DOUBLE PRECISION,
  setpoint_conductivity DOUBLE PRECISION,
  mean_wash_tank_temp DOUBLE PRECISION,
  mean_rinse_temp DOUBLE PRECISION,
  en_fill_on INTEGER,
  en_rinse_on INTEGER,
  en_wash_on INTEGER,
  version_and_message_type INTEGER,
  date_time_utc BIGINT,
  event_time_utc_ms BIGINT NOT NULL,
  kafka_logappend_ms BIGINT,
  produced_at_ms BIGINT,
  ingest_at TIMESTAMPTZ,
  persisted_at TIMESTAMPTZ,
  event_latency_seconds DOUBLE PRECISION,
  latency_seconds DOUBLE PRECISION,
  fal_seconds DOUBLE PRECISION,
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
  flags_computed_at TIMESTAMP,
  stream_features_ready_at TIMESTAMPTZ,
  window_integrity_5min DOUBLE PRECISION,
  field_consistency_score DOUBLE PRECISION,
  missing_rate DOUBLE PRECISION,
  wash_temp_diff DOUBLE PRECISION,
  rinse_temp_diff DOUBLE PRECISION,
  conductivity_diff DOUBLE PRECISION,
  wash_temp_mean_5min DOUBLE PRECISION,
  wash_temp_min_5min DOUBLE PRECISION,
  wash_temp_max_5min DOUBLE PRECISION,
  rinse_temp_mean_5min DOUBLE PRECISION,
  rinse_temp_min_5min DOUBLE PRECISION,
  rinse_temp_max_5min DOUBLE PRECISION,
  conductivity_mean_5min DOUBLE PRECISION,
  conductivity_min_5min DOUBLE PRECISION,
  conductivity_max_5min DOUBLE PRECISION,
  wash_temp_delta_30s DOUBLE PRECISION,
  conductivity_delta_30s DOUBLE PRECISION,
  features_computed_at TIMESTAMPTZ,
  features_ready_ms BIGINT,
  scored_at_ms BIGINT
);

CREATE INDEX IF NOT EXISTS idx_dishwasher_device_id ON dishwasher_data(device_id);
CREATE INDEX IF NOT EXISTS idx_dishwasher_mac ON dishwasher_data(mac_address_string);
CREATE INDEX IF NOT EXISTS idx_dishwasher_datetime_utc ON dishwasher_data(date_time_utc);
CREATE INDEX IF NOT EXISTS idx_dishwasher_features_computed ON dishwasher_data(features_computed_at);
CREATE INDEX IF NOT EXISTS idx_dishwasher_event_time_utc_ms ON dishwasher_data(event_time_utc_ms);
CREATE INDEX IF NOT EXISTS idx_dishwasher_kafka_logappend_ms ON dishwasher_data(kafka_logappend_ms);
CREATE INDEX IF NOT EXISTS idx_dishwasher_ingest_at ON dishwasher_data(ingest_at);
CREATE INDEX IF NOT EXISTS idx_dishwasher_persisted_at ON dishwasher_data(persisted_at);

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
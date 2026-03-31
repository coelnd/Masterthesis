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
  id SERIAL PRIMARY KEY,
  event_id UUID UNIQUE,
  device_id TEXT,
  mac_address_string TEXT,
  domain TEXT GENERATED ALWAYS AS (
    CASE ABS(hashtext(COALESCE(mac_address_string, ''))) % 3
      WHEN 0 THEN 'washing'
      WHEN 1 THEN 'rinsing'
      ELSE 'chemistry'
    END
  ) STORED,
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
  scored_at_ms BIGINT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dishwasher_device_id ON dishwasher_data(device_id);
CREATE INDEX IF NOT EXISTS idx_dishwasher_mac ON dishwasher_data(mac_address_string);
CREATE INDEX IF NOT EXISTS idx_dishwasher_domain ON dishwasher_data(domain);
CREATE INDEX IF NOT EXISTS idx_dishwasher_datetime_utc ON dishwasher_data(date_time_utc);
CREATE INDEX IF NOT EXISTS idx_dishwasher_features_computed ON dishwasher_data(features_computed_at);
CREATE INDEX IF NOT EXISTS idx_dishwasher_event_time_utc_ms ON dishwasher_data(event_time_utc_ms);
CREATE INDEX IF NOT EXISTS idx_dishwasher_kafka_logappend_ms ON dishwasher_data(kafka_logappend_ms);
CREATE INDEX IF NOT EXISTS idx_dishwasher_ingest_at ON dishwasher_data(ingest_at);
CREATE INDEX IF NOT EXISTS idx_dishwasher_persisted_at ON dishwasher_data(persisted_at);
CREATE OR REPLACE VIEW mesh_washing_product AS
SELECT * FROM dishwasher_data WHERE domain = 'washing';

CREATE OR REPLACE VIEW mesh_rinsing_product AS
SELECT * FROM dishwasher_data WHERE domain = 'rinsing';

CREATE OR REPLACE VIEW mesh_chemistry_product AS
SELECT * FROM dishwasher_data WHERE domain = 'chemistry';

CREATE TABLE IF NOT EXISTS mesh_quality_gates (
  domain TEXT PRIMARY KEY,
  last_gate_run TIMESTAMPTZ,
  missing_rate_avg DOUBLE PRECISION,
  field_consistency_avg DOUBLE PRECISION,
  status TEXT DEFAULT 'unknown',
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE VIEW mesh_quality_snapshot AS
SELECT
  domain,
  AVG(COALESCE(missing_rate, 0.0)) AS missing_rate_avg,
  AVG(COALESCE(field_consistency_score, 0.5)) AS field_consistency_avg,
  COUNT(*) AS sample_count
FROM dishwasher_data
GROUP BY domain;
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
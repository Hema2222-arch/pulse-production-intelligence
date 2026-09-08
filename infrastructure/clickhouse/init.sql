CREATE DATABASE IF NOT EXISTS pulse;

CREATE TABLE IF NOT EXISTS pulse.telemetry (
  event_id String,
  ts DateTime64(3),
  service LowCardinality(String),
  endpoint String,
  event_type LowCardinality(String),
  latency_ms Float64,
  status UInt16,
  trace_id String,
  span_id String,
  function_name String,
  dependency String
) ENGINE = MergeTree
ORDER BY (service, ts);

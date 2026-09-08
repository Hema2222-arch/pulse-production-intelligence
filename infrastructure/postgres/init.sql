CREATE TABLE IF NOT EXISTS telemetry_events (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT UNIQUE NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  service TEXT NOT NULL,
  endpoint TEXT,
  event_type TEXT NOT NULL,
  latency_ms DOUBLE PRECISION,
  status INTEGER,
  trace_id TEXT,
  span_id TEXT,
  function_name TEXT,
  dependency TEXT,
  metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_events_service_ts ON telemetry_events(service, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON telemetry_events(event_type, ts DESC);

CREATE TABLE IF NOT EXISTS deployments (
  id BIGSERIAL PRIMARY KEY,
  deployment_id TEXT UNIQUE NOT NULL,
  service TEXT NOT NULL,
  version TEXT NOT NULL,
  deployed_at TIMESTAMPTZ NOT NULL,
  commit_sha TEXT
);

CREATE TABLE IF NOT EXISTS incidents (
  id BIGSERIAL PRIMARY KEY,
  incident_key TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL,
  severity TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'OPEN',
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  confidence DOUBLE PRECISION DEFAULT 0,
  root_cause TEXT,
  root_cause_service TEXT,
  triggering_deployment TEXT,
  timeline JSONB DEFAULT '[]'::jsonb,
  affected_services JSONB DEFAULT '[]'::jsonb,
  evidence JSONB DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_incidents_started ON incidents(started_at DESC);

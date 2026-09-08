# PULSE — Real-Time Production Incident Intelligence Platform

PULSE is an educational, runnable MVP of a production incident intelligence platform.

## What is included

- Demo e-commerce microservices: gateway, order, payment, inventory
- Telemetry generator with controllable incidents
- Kafka-compatible event bus using Redpanda in Docker Compose
- Rust stream processor
- EWMA + rolling statistics anomaly detection
- PostgreSQL incident/event storage
- ClickHouse telemetry storage
- Redis-ready infrastructure
- Dependency graph
- Incident correlation and severity
- Deployment correlation
- Root-cause candidate ranking
- Rust REST API
- React + TypeScript dashboard
- One-command Docker startup
- Load/incident simulation scripts

## Quick start

Requirements:
- Docker Desktop / Docker Engine + Compose
- Git
- Optional: Rust and Node.js for local development

Run:

```bash
docker compose up --build
```

Then open:
- Dashboard: http://localhost:3000
- API: http://localhost:8080/health
- API docs-like endpoints: http://localhost:8080/api/incidents
- Redpanda console: http://localhost:8081
- ClickHouse HTTP: http://localhost:8123

Generate normal traffic:

```bash
python scripts/generate_traffic.py --seconds 60
```

Trigger the demo database regression:

```bash
python scripts/trigger_incident.py
```

Watch the dashboard. The intended chain is:

database slowdown -> payment latency/error spike -> order timeout -> checkout failures

## Architecture

```text
Demo services
   |
   +-- telemetry --> Kafka/Redpanda --> Rust stream processor
   |                                      |
   |                                      +--> anomaly detector
   |                                      +--> dependency graph
   |                                      +--> incident engine
   |                                      +--> root cause ranking
   |                                      |
   |                                      +--> PostgreSQL
   |                                      +--> ClickHouse
   |
   +----------------------------------------------+
                                                  |
                                             Rust API
                                                  |
                                             React UI
```

## Important engineering note

Root cause is a ranked hypothesis, not proof. PULSE combines temporal correlation, anomaly magnitude, dependency topology, trace evidence, and deployment proximity to calculate a confidence score.

## Development roadmap

1. MVP telemetry + anomaly detection
2. Distributed tracing with OpenTelemetry
3. Real source-code/function mapping
4. More robust change-point detection
5. Kubernetes deployment
6. Load testing and benchmark suite
7. Failure propagation simulation

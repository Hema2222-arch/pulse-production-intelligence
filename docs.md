# PULSE Engineering Notes

## Core data model

Telemetry events are immutable facts. An incident is a derived object. This separation makes reprocessing possible.

## Anomaly detector

The MVP uses a bounded rolling window and a 3-sigma rule. EWMA is included as the next extension point. For a production implementation, use percentile baselines by service/endpoint/hour-of-week and combine them with change-point detection.

## Root-cause ranking

Recommended score:

`score = 0.30 temporal_proximity + 0.25 anomaly_magnitude + 0.20 dependency_centrality + 0.15 trace_contribution + 0.10 deployment_proximity`

Never describe this score as proof. It is a hypothesis ranking.

## Next implementation steps

- Add deployment ingestion and a deployment API.
- Persist service graph edges from trace spans.
- Add trace waterfall endpoint.
- Calculate affected services with reverse graph traversal.
- Group incidents with a time-window + shared dependency algorithm.
- Add a simulation endpoint to estimate propagation.
- Add OpenTelemetry Collector.
- Add benchmark harness for 1k/10k/100k events/sec.

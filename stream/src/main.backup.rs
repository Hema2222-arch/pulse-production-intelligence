use anyhow::Result;
use chrono::{DateTime, Utc};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{collections::HashMap, env, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    event_id: String,
    timestamp: DateTime<Utc>,
    service: String,
    endpoint: Option<String>,
    event_type: String,
    latency_ms: Option<f64>,
    status: Option<u16>,
    trace_id: Option<String>,
    span_id: Option<String>,
    function_name: Option<String>,
    dependency: Option<String>,
    metadata: serde_json::Value,
}

#[derive(Default)]
struct Window {
    values: Vec<f64>,
}

impl Window {
    fn push(&mut self, x: f64) {
        self.values.push(x);

        if self.values.len() > 120 {
            self.values.remove(0);
        }
    }

    fn mean(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }

        self.values.iter().sum::<f64>() / self.values.len() as f64
    }

    fn stddev(&self) -> f64 {
        if self.values.len() < 2 {
            return 0.0;
        }

        let m = self.mean();

        (
            self.values
                .iter()
                .map(|x| (x - m).powi(2))
                .sum::<f64>()
                / self.values.len() as f64
        )
            .sqrt()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let brokers =
        env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());

    let db =
        env::var("DATABASE_URL").expect("DATABASE_URL required");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&db)
        .await?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "pulse-processor")
        .set("bootstrap.servers", &brokers)
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["telemetry", "deployments"])?;

    let windows: Arc<RwLock<HashMap<String, Window>>> =
        Arc::new(RwLock::new(HashMap::new()));

    println!("PULSE stream processor listening on {brokers}");

    loop {
        match consumer.recv().await {
            Err(e) => {
                eprintln!("Kafka error: {e}");
            }

            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<Event>(payload) {
                        Ok(event) => {
                            if let Err(e) =
                                process_event(&pool, &windows, &event).await
                            {
                                eprintln!("event processing error: {e}");
                            }
                        }

                        Err(e) => {
                            eprintln!("invalid telemetry JSON: {e}");
                        }
                    }
                }
            }
        }
    }
}

async fn process_event(
    pool: &PgPool,
    windows: &Arc<RwLock<HashMap<String, Window>>>,
    event: &Event,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO telemetry_events
        (
            event_id,
            ts,
            service,
            endpoint,
            event_type,
            latency_ms,
            status,
            trace_id,
            span_id,
            function_name,
            dependency,
            metadata
        )
        VALUES
        (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
        )
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(&event.event_id)
    .bind(event.timestamp)
    .bind(&event.service)
    .bind(&event.endpoint)
    .bind(&event.event_type)
    .bind(event.latency_ms)
    .bind(event.status.map(|x| x as i32))
    .bind(&event.trace_id)
    .bind(&event.span_id)
    .bind(&event.function_name)
    .bind(&event.dependency)
    .bind(&event.metadata)
    .execute(pool)
    .await?;

    if let Some(latency) = event.latency_ms {
        let key = format!(
            "{}:{}",
            event.service,
            event.endpoint.clone().unwrap_or_default()
        );

        let mut map = windows.write().await;

        let window = map.entry(key).or_default();

        let baseline = window.mean();
        let sigma = window.stddev();

        window.push(latency);

        if window.values.len() >= 20
            && sigma > 0.0
            && latency > baseline + 3.0 * sigma
        {
            let severity =
                if latency > baseline.max(1.0) * 10.0 {
                    "CRITICAL"
                } else {
                    "HIGH"
                };

            create_or_update_incident(
                pool,
                event,
                severity,
                baseline,
                latency,
            )
            .await?;
        }
    }

    Ok(())
}

async fn create_or_update_incident(
    pool: &PgPool,
    event: &Event,
    severity: &str,
    baseline: f64,
    observed: f64,
) -> Result<()> {
    let key = format!(
        "anomaly:{}:{}",
        event.service,
        event.endpoint.clone().unwrap_or_default()
    );

    let title =
        format!("{} latency anomaly", event.service);

    let evidence = serde_json::json!([
        {
            "kind": "latency_anomaly",
            "baseline_ms": baseline,
            "observed_ms": observed,
            "service": event.service,
            "endpoint": event.endpoint
        }
    ]);

    sqlx::query(
        r#"
        INSERT INTO incidents
        (
            incident_key,
            title,
            severity,
            status,
            started_at,
            confidence,
            root_cause,
            root_cause_service,
            affected_services,
            evidence
        )
        VALUES
        (
            $1,
            $2,
            $3,
            'OPEN',
            $4,
            0.55,
            'Investigate latency anomaly',
            $5,
            '[]',
            $6
        )
        ON CONFLICT (incident_key) DO UPDATE SET
            severity = EXCLUDED.severity,
            evidence = EXCLUDED.evidence
        "#,
    )
    .bind(&key)
    .bind(&title)
    .bind(severity)
    .bind(event.timestamp)
    .bind(&event.service)
    .bind(evidence)
    .execute(pool)
    .await?;

    // THIS IS WHAT MAKES THE INCIDENT REAL-TIME.
    sqlx::query(
        "SELECT pg_notify('pulse_incidents', $1)"
    )
    .bind(&key)
    .execute(pool)
    .await?;

    println!(
        "PULSE incident updated: {} [{}]",
        key, severity
    );

    Ok(())
}
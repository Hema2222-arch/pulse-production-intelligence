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
    fn push(&mut self, value: f64) {
        self.values.push(value);

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

        let mean = self.mean();

        (self.values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / self.values.len() as f64)
            .sqrt()
    }
}

#[derive(Default)]
struct CorrelationState {
    database_anomaly: Option<(DateTime<Utc>, f64)>,
    payment_anomaly: Option<(DateTime<Utc>, f64)>,
    active_payment_incident: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL required");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "pulse-processor")
        .set("bootstrap.servers", &brokers)
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["telemetry", "deployments"])?;

    let windows: Arc<RwLock<HashMap<String, Window>>> = Arc::new(RwLock::new(HashMap::new()));

    let correlation: Arc<RwLock<CorrelationState>> =
        Arc::new(RwLock::new(CorrelationState::default()));

    println!("PULSE stream processor listening on {brokers}");

    loop {
        match consumer.recv().await {
            Err(error) => {
                eprintln!("Kafka error: {error}");
            }

            Ok(message) => {
                let Some(payload) = message.payload() else {
                    continue;
                };

                let event = match serde_json::from_slice::<Event>(payload) {
                    Ok(event) => event,
                    Err(error) => {
                        eprintln!("Invalid telemetry JSON: {error}");
                        continue;
                    }
                };

                if let Err(error) = process_event(&pool, &windows, &correlation, &event).await {
                    eprintln!("PULSE event processing error: {error}");
                }
            }
        }
    }
}

async fn process_event(
    pool: &PgPool,
    windows: &Arc<RwLock<HashMap<String, Window>>>,
    correlation: &Arc<RwLock<CorrelationState>>,
    event: &Event,
) -> Result<()> {
    // ------------------------------------------------------------
    // 1. Persist telemetry
    // ------------------------------------------------------------

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

    let Some(latency) = event.latency_ms else {
        return Ok(());
    };

    // ------------------------------------------------------------
    // 2. Rolling baseline + anomaly detection
    // ------------------------------------------------------------

    let key = format!(
        "{}:{}",
        event.service,
        event.endpoint.clone().unwrap_or_default()
    );

    let (baseline, sigma, anomaly, normal) = {
        let mut map = windows.write().await;

        let window = map.entry(key).or_default();

        let baseline = window.mean();
        let sigma = window.stddev();
        let sample_count = window.values.len();

        // Statistical anomaly.
        let statistical_anomaly =
            sample_count >= 20 && sigma > 0.0 && latency > baseline + (3.0 * sigma);

        // Hard safety threshold for clearly pathological latency.
        let threshold_anomaly = (event.service == "postgres" && latency >= 500.0)
            || (event.service == "payment" && latency >= 500.0);

        let anomaly = threshold_anomaly || statistical_anomaly;

        // Recovery requires a genuinely healthy signal.
        let normal = sample_count >= 20 && latency < 200.0 && event.status == Some(200);

        window.push(latency);

        (baseline, sigma, anomaly, normal)
    };

    println!(
        "Telemetry: service={} latency={}ms anomaly={}",
        event.service, latency, anomaly
    );

    // ------------------------------------------------------------
    // 3. Capture database anomaly
    // ------------------------------------------------------------

    if event.service == "postgres" && anomaly {
        let mut state = correlation.write().await;

        state.database_anomaly = Some((event.timestamp, latency));

        println!("DATABASE ANOMALY detected: {} ms", latency);
    }

    // ------------------------------------------------------------
    // 4. Capture payment anomaly
    // ------------------------------------------------------------

    if event.service == "payment" && anomaly {
        let mut state = correlation.write().await;

        state.payment_anomaly = Some((event.timestamp, latency));

        println!("PAYMENT ANOMALY detected: {} ms", latency);
    }

    // ------------------------------------------------------------
    // 5. Correlate database -> payment
    // ------------------------------------------------------------

    if event.service == "payment" && anomaly {
        let mut state = correlation.write().await;

        let root_cause_is_database =
            if let Some((database_time, database_latency)) = state.database_anomaly {
                let gap_seconds = event
                    .timestamp
                    .signed_duration_since(database_time)
                    .num_seconds()
                    .abs();

                gap_seconds <= 30 && database_latency >= 500.0
            } else {
                false
            };

        let (confidence, root_cause, root_cause_service) = if root_cause_is_database {
            (
                0.88,
                "Database degradation is the most likely root cause",
                "postgres",
            )
        } else {
            (0.55, "Investigate latency anomaly", "payment")
        };

        let severity = if latency >= baseline.max(1.0) * 10.0 {
            "CRITICAL"
        } else {
            "HIGH"
        };

        let incident_key = "anomaly:payment:/charge";

        let evidence = serde_json::json!([
            {
                "kind": "latency_anomaly",
                "service": "payment",
                "endpoint": "/charge",
                "baseline_ms": baseline,
                "observed_ms": latency,
                "sigma": sigma
            },
            {
                "kind": "dependency_correlation",
                "dependency": "database",
                "database_anomaly": root_cause_is_database,
                "root_cause_service": root_cause_service,
                "confidence": confidence
            }
        ]);

        let affected_services = serde_json::json!(["payment", "order"]);

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
                'Payment latency anomaly',
                $2,
                'OPEN',
                $3,
                $4,
                $5,
                $6,
                $7,
                $8
            )
            ON CONFLICT (incident_key)
            DO UPDATE SET
                severity = EXCLUDED.severity,
                status = 'OPEN',
                confidence = EXCLUDED.confidence,
                root_cause = EXCLUDED.root_cause,
                root_cause_service = EXCLUDED.root_cause_service,
                affected_services = EXCLUDED.affected_services,
                evidence = EXCLUDED.evidence
            "#,
        )
        .bind(incident_key)
        .bind(severity)
        .bind(event.timestamp)
        .bind(confidence)
        .bind(root_cause)
        .bind(root_cause_service)
        .bind(affected_services)
        .bind(evidence)
        .execute(pool)
        .await?;

        state.active_payment_incident = Some(incident_key.to_string());

        sqlx::query("SELECT pg_notify('pulse_incidents', $1)")
            .bind(incident_key)
            .execute(pool)
            .await?;

        println!(
            "PULSE ROOT CAUSE: {} ({:.0}% confidence)",
            root_cause,
            confidence * 100.0
        );
    }

    // ------------------------------------------------------------
    // 6. Automatic recovery
    // ------------------------------------------------------------

    if event.service == "payment" && normal {
        let mut state = correlation.write().await;

        if let Some(incident_key) = state.active_payment_incident.clone() {
            sqlx::query(
                r#"
                UPDATE incidents
                SET
                    status = 'RESOLVED',
                    ended_at = $2
                WHERE incident_key = $1
                  AND status = 'OPEN'
                "#,
            )
            .bind(&incident_key)
            .bind(event.timestamp)
            .execute(pool)
            .await?;

            sqlx::query("SELECT pg_notify('pulse_incidents', $1)")
                .bind(&incident_key)
                .execute(pool)
                .await?;

            println!("PULSE INCIDENT RESOLVED: {}", incident_key);

            state.active_payment_incident = None;
            state.payment_anomaly = None;
            state.database_anomaly = None;
        }
    }

    Ok(())
}

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Clone)]
struct App {
    producer: FutureProducer,
    degraded: Arc<AtomicBool>,
}

#[derive(Deserialize)]
struct Charge {
    amount: f64,
}

#[derive(Serialize)]
struct Response {
    ok: bool,
    trace_id: String,
    latency_ms: u64,
}

#[derive(Serialize)]
struct IncidentStatus {
    database_degraded: bool,
}

#[tokio::main]
async fn main() {
    let brokers = env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "localhost:9092".into());

    let producer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("Kafka producer creation failed");

    let state = App {
        producer,
        degraded: Arc::new(AtomicBool::new(false)),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/charge", post(charge))
        .route("/incident/start", post(start_incident))
        .route("/incident/stop", post(stop_incident))
        .route("/incident/status", get(incident_status))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7001")
        .await
        .expect("Failed to bind port 7001");

    println!("PULSE payment service listening on :7001");

    axum::serve(listener, app)
        .await
        .expect("Payment service failed");
}

async fn health() -> &'static str {
    "ok"
}

async fn start_incident(
    State(app): State<App>,
) -> Json<IncidentStatus> {
    app.degraded.store(true, Ordering::SeqCst);

    println!("DATABASE DEGRADATION STARTED");

    Json(IncidentStatus {
        database_degraded: true,
    })
}

async fn stop_incident(
    State(app): State<App>,
) -> Json<IncidentStatus> {
    app.degraded.store(false, Ordering::SeqCst);

    println!("DATABASE DEGRADATION RECOVERED");

    Json(IncidentStatus {
        database_degraded: false,
    })
}

async fn incident_status(
    State(app): State<App>,
) -> Json<IncidentStatus> {
    Json(IncidentStatus {
        database_degraded: app.degraded.load(Ordering::SeqCst),
    })
}

async fn charge(
    State(app): State<App>,
    Json(request): Json<Charge>,
) -> Json<Response> {
    let trace_id = Uuid::new_v4().to_string();

    let database_degraded =
        app.degraded.load(Ordering::SeqCst);

    // Simulated database operation.
    let database_start = std::time::Instant::now();

    if database_degraded {
        sleep(Duration::from_millis(750)).await;
    } else {
        sleep(Duration::from_millis(40)).await;
    }

    let database_latency =
        database_start.elapsed().as_millis() as u64;

    // ---------------------------------------------------------
    // DATABASE TELEMETRY
    // ---------------------------------------------------------

    let database_event = serde_json::json!({
        "event_id": Uuid::new_v4().to_string(),
        "timestamp": Utc::now(),
        "service": "postgres",
        "endpoint": "/query",
        "event_type": "dependency",
        "latency_ms": database_latency,
        "status": if database_degraded { 500 } else { 200 },
        "trace_id": trace_id,
        "span_id": Uuid::new_v4().to_string(),
        "function_name": "PaymentRepository.getPaymentMethod()",
        "dependency": "database",
        "metadata": {
            "simulated": true,
            "source_service": "payment",
            "amount": request.amount
        }
    });

    let _ = app
        .producer
        .send(
            FutureRecord::to("telemetry")
                .payload(&database_event.to_string())
                .key("postgres"),
            Duration::from_secs(1),
        )
        .await;

    // ---------------------------------------------------------
    // PAYMENT REQUEST
    // ---------------------------------------------------------

    let payment_start = std::time::Instant::now();

    if database_degraded {
        sleep(Duration::from_millis(150)).await;
    } else {
        sleep(Duration::from_millis(5)).await;
    }

    let payment_latency =
        payment_start.elapsed().as_millis() as u64
            + database_latency;

    let status = if database_degraded { 500 } else { 200 };

    // ---------------------------------------------------------
    // PAYMENT TELEMETRY
    // ---------------------------------------------------------

    let payment_event = serde_json::json!({
        "event_id": Uuid::new_v4().to_string(),
        "timestamp": Utc::now(),
        "service": "payment",
        "endpoint": "/charge",
        "event_type": "http",
        "latency_ms": payment_latency,
        "status": status,
        "trace_id": trace_id,
        "span_id": Uuid::new_v4().to_string(),
        "function_name": "PaymentRepository.getPaymentMethod()",
        "dependency": "database",
        "metadata": {
            "database_latency_ms": database_latency,
            "simulated_database_incident": database_degraded
        }
    });

    let _ = app
        .producer
        .send(
            FutureRecord::to("telemetry")
                .payload(&payment_event.to_string())
                .key("payment"),
            Duration::from_secs(1),
        )
        .await;

    Json(Response {
        ok: !database_degraded,
        trace_id,
        latency_ms: payment_latency,
    })
}
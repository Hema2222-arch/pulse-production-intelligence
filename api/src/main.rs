use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{convert::Infallible, env, time::Duration};
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tower_http::cors::CorsLayer;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    updates: broadcast::Sender<String>,
}

#[derive(Serialize, sqlx::FromRow)]
struct Incident {
    id: i64,
    incident_key: String,
    title: String,
    severity: String,
    status: String,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
    confidence: f64,
    root_cause: Option<String>,
    root_cause_service: Option<String>,
    triggering_deployment: Option<String>,
    timeline: serde_json::Value,
    affected_services: serde_json::Value,
    evidence: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    // ------------------------------------------------------------
    // DATABASE
    // ------------------------------------------------------------

    let url = env::var("DATABASE_URL")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await?;

    // ------------------------------------------------------------
    // REAL-TIME UPDATE CHANNEL
    // ------------------------------------------------------------

    let (updates, _) = broadcast::channel::<String>(200);

    let state = AppState {
        pool: pool.clone(),
        updates: updates.clone(),
    };

    // ------------------------------------------------------------
    // POSTGRES NOTIFY -> SSE
    // ------------------------------------------------------------

    let listener_pool = pool.clone();
    let listener_updates = updates.clone();

    tokio::spawn(async move {
        loop {
            match sqlx::postgres::PgListener::connect_with(&listener_pool).await {
                Ok(mut listener) => {
                    if let Err(e) = listener.listen("pulse_incidents").await {
                        eprintln!("PULSE notify listener setup error: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }

                    println!("PULSE realtime listener connected");

                    loop {
                        match listener.recv().await {
                            Ok(notification) => {
                                let payload = notification.payload().to_string();

                                let message = serde_json::json!({
                                    "type": "incident_changed",
                                    "incident_key": payload,
                                    "timestamp": Utc::now()
                                })
                                .to_string();

                                let _ = listener_updates.send(message);
                            }

                            Err(e) => {
                                eprintln!("PULSE notify listener error: {e}");
                                break;
                            }
                        }
                    }
                }

                Err(e) => {
                    eprintln!("PULSE notify listener connection error: {e}");
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // ------------------------------------------------------------
    // ROUTES
    // ------------------------------------------------------------

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/incidents", get(incidents))
        .route("/api/topology", get(topology))
        .route("/api/events/recent", get(recent_events))
        .route("/api/stream", get(stream))
        .with_state(state)
        .layer(CorsLayer::permissive());

    // ------------------------------------------------------------
    // CLOUD/LOCAL PORT
    // ------------------------------------------------------------

    // Render provides PORT.
    // Locally, default to 8080.
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());

    let addr = format!("0.0.0.0:{port}");

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    println!("PULSE API listening on {addr}");

    // IMPORTANT: only ONE axum::serve call.
    axum::serve(listener, app).await?;

    Ok(())
}

// ------------------------------------------------------------
// HEALTH
// ------------------------------------------------------------

async fn health() -> &'static str {
    "ok"
}

// ------------------------------------------------------------
// INCIDENTS
// ------------------------------------------------------------

async fn incidents(State(state): State<AppState>) -> Result<Json<Vec<Incident>>, StatusCode> {
    sqlx::query_as::<_, Incident>(
        r#"
        SELECT
            id,
            incident_key,
            title,
            severity,
            status,
            started_at,
            ended_at,
            confidence,
            root_cause,
            root_cause_service,
            triggering_deployment,
            timeline,
            affected_services,
            evidence
        FROM incidents
        ORDER BY started_at DESC
        LIMIT 50
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map(Json)
    .map_err(|e| {
        eprintln!("incidents query error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

// ------------------------------------------------------------
// TOPOLOGY
// ------------------------------------------------------------

async fn topology() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "nodes": [
            {"id": "gateway", "label": "API Gateway"},
            {"id": "order", "label": "Order"},
            {"id": "payment", "label": "Payment"},
            {"id": "inventory", "label": "Inventory"},
            {"id": "database", "label": "Database"}
        ],
        "edges": [
            {"source": "gateway", "target": "order"},
            {"source": "order", "target": "payment"},
            {"source": "order", "target": "inventory"},
            {"source": "payment", "target": "database"}
        ]
    }))
}

// ------------------------------------------------------------
// RECENT EVENTS
// ------------------------------------------------------------

async fn recent_events(
    State(state): State<AppState>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    let rows = sqlx::query_as::<_, (String, DateTime<Utc>, String, Option<f64>, Option<i32>)>(
        r#"
        SELECT
            event_id,
            ts,
            service,
            latency_ms,
            status
        FROM telemetry_events
        ORDER BY ts DESC
        LIMIT 100
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        eprintln!("recent events query error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let events = rows
        .into_iter()
        .map(|(id, ts, service, latency, status)| {
            serde_json::json!({
                "event_id": id,
                "timestamp": ts,
                "service": service,
                "latency_ms": latency,
                "status": status
            })
        })
        .collect();

    Ok(Json(events))
}

// ------------------------------------------------------------
// REAL-TIME SERVER-SENT EVENTS
// ------------------------------------------------------------

async fn stream(State(state): State<AppState>) -> impl IntoResponse {
    let receiver = state.updates.subscribe();

    let stream = BroadcastStream::new(receiver).filter_map(|result| match result {
        Ok(payload) => Some(Ok::<Event, Infallible>(
            Event::default().event("incident_changed").data(payload),
        )),
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(5))
            .text("keepalive"),
    )
}

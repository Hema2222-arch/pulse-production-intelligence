Invoke-WebRequest http://localhost:7001/charge -Method POST -ContentType "application/json" -Body '{"amount":100}' -UseBasicParsinguse axum::{routing::{get,post},Json,Router};
use serde::{Deserialize,Serialize};
use std::env;
#[derive(Deserialize,Serialize)]struct Req{amount:f64}
#[tokio::main]async fn main(){let a=Router::new().route("/health",get(||async{"ok"})).route("/checkout",post(checkout));let l=tokio::net::TcpListener::bind("0.0.0.0:7000").await.unwrap();axum::serve(l,a).await.unwrap();}
async fn checkout(Json(req):Json<Req>)->Json<serde_json::Value>{let u=env::var("ORDER_URL").unwrap_or("http://localhost:7002".into());match reqwest::Client::new().post(format!("{u}/orders")).json(&req).send().await{Ok(r)=>Json(serde_json::json!({"status":r.status().as_u16()})),Err(_)=>Json(serde_json::json!({"status":503}))}}

use axum::{routing::get,Router};
#[tokio::main]async fn main(){let a=Router::new().route("/health",get(||async{"ok"}));let l=tokio::net::TcpListener::bind("0.0.0.0:7003").await.unwrap();axum::serve(l,a).await.unwrap();}

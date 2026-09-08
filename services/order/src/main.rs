use axum::{routing::{get,post},Json,Router};
use serde::{Deserialize,Serialize};
use std::env;
#[derive(Deserialize, Serialize)]
struct Req {
    amount: f64,
}#[derive(Serialize)] struct Resp{ok:bool,message:String}
#[tokio::main] async fn main(){
 let app=Router::new().route("/health",get(||async{"ok"})).route("/orders",post(order));
 let l=tokio::net::TcpListener::bind("0.0.0.0:7002").await.unwrap(); axum::serve(l,app).await.unwrap();
}
async fn order(Json(req):Json<Req>)->Json<Resp>{
 let url=env::var("PAYMENT_URL").unwrap_or("http://localhost:7001".into());
 let client=reqwest::Client::new();
 let r=client.post(format!("{url}/charge")).json(&req).send().await;
 match r {Ok(x) if x.status().is_success()=>Json(Resp{ok:true,message:"order created".into()}),_=>Json(Resp{ok:false,message:"payment timeout/failure".into()})}
}

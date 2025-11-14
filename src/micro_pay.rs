use std::time::Duration;

use color_eyre::{Result, eyre::eyre};
use serde_json::Value;

pub async fn payment_prepare(url: &str, body: &Value) -> Result<Value> {
    reqwest::Client::new()
        .post(format!("{url}/api/payment/prepare"))
        .header("Content-Type", "application/json; charset=utf-8")
        .body(body.to_string())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

pub async fn payment_transfer(url: &str, body: &Value) -> Result<Value> {
    reqwest::Client::new()
        .post(format!("{url}/api/payment/transfer"))
        .header("Content-Type", "application/json; charset=utf-8")
        .body(body.to_string())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

pub async fn payment_completed_total(url: &str, info: &str) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/completed-total?info={info}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

pub async fn payment_completed(url: &str, query: &str) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/completed?{query}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

pub async fn payment_sender_did(
    url: &str,
    sender_did: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/sender-did/{sender_did}"))
        .query(query)
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

pub async fn payment_receiver_did(
    url: &str,
    receiver_did: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/receiver-did/{receiver_did}"))
        .query(query)
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

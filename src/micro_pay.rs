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

#[allow(dead_code)]
pub async fn payment(url: &str, payment_id: &str) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/{payment_id}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

#[allow(dead_code)]
pub async fn payment_sender(url: &str, sender: &str) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/sender/{sender}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

#[allow(dead_code)]
pub async fn payment_receiver(url: &str, receiver: &str) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/api/payment/receiver/{receiver}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call micro_pay failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode micro_pay response failed: {e}"))
}

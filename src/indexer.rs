use std::time::Duration;

use color_eyre::{Result, eyre::eyre};
use serde_json::Value;

pub async fn did_document(url: &str, did: &str) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/{did}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call indexer failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode indexer response failed: {e}"))
}

#[allow(dead_code)]
pub async fn ckb_did(url: &str, ckb_addr: &str) -> Result<String> {
    reqwest::Client::new()
        .get(format!("{url}/resolve-ckb-addr/{ckb_addr}"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call indexer failed: {e}"))?
        .text()
        .await
        .map_err(|e| eyre!("decode indexer response failed: {e}"))
}

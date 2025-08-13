use std::time::Duration;

use color_eyre::{Result, eyre::eyre};
use serde_json::{Value, json};

pub const NSID_POST: &str = "app.bbs.post";
pub const NSID_PROFILE: &str = "app.actor.profile";

#[allow(dead_code)]
pub async fn create_record(
    url: &str,
    auth: &str,
    repo: &str,
    nsid: &str,
    record: &Value,
) -> Result<Value> {
    reqwest::Client::new()
        .post(format!("{url}/xrpc/com.atproto.repo.createRecord"))
        .bearer_auth(auth)
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .body(
            json!({
                "repo": repo,
                "collection": nsid,
                "validate": false,
                "record": record,
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|e| eyre!("call pds failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode pds response failed: {e}"))
}

pub async fn get_record(
    url: &str,
    repo: &str,
    nsid: &str,
    rkey: &str,
) -> Result<Value> {
    reqwest::Client::new()
        .get(format!("{url}/xrpc/com.atproto.repo.getRecord"))
        .query(&[("repo", repo), ("collection", nsid), ("rkey", rkey)])
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| eyre!("call pds failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode pds response failed: {e}"))
}

#[allow(dead_code)]
pub async fn put_record(
    url: &str,
    auth: &str,
    repo: &str,
    nsid: &str,
    rkey: &str,
    record: &Value,
) -> Result<Value> {
    reqwest::Client::new()
        .post(format!("{url}/xrpc/com.atproto.repo.putRecord"))
        .bearer_auth(auth)
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .body(
            json!({
                "repo": repo,
                "collection": nsid,
                "rkey": rkey,
                "validate": false,
                "record": record,
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|e| eyre!("call pds failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode pds response failed: {e}"))
}

pub async fn direct_writes(
    url: &str,
    auth: &str,
    repo: &str,
    writes: &Value,
    signing_key: &str,
    root: &Value,
) -> Result<Value> {
    reqwest::Client::new()
        .post(format!("{url}/xrpc/com.atproto.web5.directWrites"))
        .bearer_auth(auth)
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .body(
            json!({
                "repo": repo,
                "validate": false,
                "writes": writes,
                "signingKey": signing_key,
                "root": root
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|e| eyre!("call pds failed: {e}"))?
        .json::<Value>()
        .await
        .map_err(|e| eyre!("decode pds response failed: {e}"))
}

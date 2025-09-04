use std::{str::FromStr, time::Duration};

use color_eyre::{Result, eyre::eyre};
use serde_json::{Value, json};

pub const NSID_POST: &str = "app.bbs.post";
pub const NSID_COMMENT: &str = "app.bbs.comment";
pub const NSID_REPLY: &str = "app.bbs.reply";
pub const NSID_LIKE: &str = "app.bbs.like";
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

pub async fn get_record(url: &str, repo: &str, nsid: &str, rkey: &str) -> Result<Value> {
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
    ckb_addr: &str,
    root: &Value,
) -> Result<Value> {
    let rsp = reqwest::Client::new()
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
                "root": root,
                "ckbAddr": ckb_addr,
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|e| eyre!("call pds failed: {e}"))?;
    debug!("pds rsp: {rsp:?}");
    let body_str = rsp
        .text()
        .await
        .map_err(|e| eyre!("read pds response failed: {e}"))?;
    debug!("pds rsp body: {body_str}");
    Value::from_str(&body_str).map_err(|e| eyre!("decode pds response failed: {e}"))
}

pub async fn index_query(url: &str, did: &str, item: &str) -> Result<Value> {
    let rsp = reqwest::Client::new()
        .post(format!("{url}/xrpc/com.atproto.web5.indexQuery"))
        .header("Content-Type", "application/json; charset=utf-8")
        .timeout(Duration::from_secs(5))
        .body(
            json!({
                "index": {
                    "$type": format!("com.atproto.web5.indexQuery#{}", item),
                    "did": did,
                },
            })
            .to_string(),
        )
        .send()
        .await
        .map_err(|e| eyre!("call pds failed: {e}"))?;
    debug!("pds rsp: {rsp:?}");
    let body_str = rsp
        .text()
        .await
        .map_err(|e| eyre!("read pds response failed: {e}"))?;
    debug!("pds rsp body: {body_str}");
    Value::from_str(&body_str).map_err(|e| eyre!("decode pds response failed: {e}"))
}

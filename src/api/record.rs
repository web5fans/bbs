use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AppView,
    atproto::{NSID_LIKE, NSID_POST, NSID_REPLY, direct_writes},
    error::AppError,
    lexicon::{like::Like, post::Post, reply::Reply},
};

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct NewRecord {
    repo: String,
    rkey: String,
    value: Value,
    signing_key: String,
    ckb_addr: String,
    root: Value,
}

pub(crate) async fn create(
    State(state): State<AppView>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Json(new_record): Json<NewRecord>,
) -> Result<impl IntoResponse, AppError> {
    let record_type = new_record
        .value
        .get("$type")
        .map(|t| t.as_str())
        .ok_or_eyre("'$type' must be set")?
        .ok_or_eyre("'$type' must be set")?;
    if !state.whitelist.contains(&new_record.repo) {
        match record_type {
            NSID_POST | NSID_REPLY => return Err(eyre!("Operation is not allowed!").into()),
            _ => {}
        }
    }
    let result = direct_writes(
        &state.pds,
        auth.token(),
        &new_record.repo,
        &json!([{
            "$type": "com.atproto.web5.directWrites#create",
            "collection": new_record.value["$type"],
            "rkey": new_record.rkey,
            "value": new_record.value
        }]),
        &new_record.signing_key,
        &new_record.ckb_addr,
        &new_record.root,
    )
    .await
    .map_err(|e| AppError::CallPdsFailed(e.to_string()))?;
    debug!("pds: {}", result);
    let uri = result
        .pointer("/results/0/uri")
        .and_then(|uri| uri.as_str())
        .ok_or(AppError::CallPdsFailed(result.to_string()))?;
    let cid = result
        .pointer("/results/0/cid")
        .and_then(|cid| cid.as_str())
        .ok_or(AppError::CallPdsFailed(result.to_string()))?;
    match record_type {
        NSID_POST => {
            Post::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
        }
        NSID_REPLY => {
            Reply::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
        }
        NSID_LIKE => {
            Like::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
        }
        _ => {}
    }

    Ok(ok(result))
}

use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use color_eyre::eyre::OptionExt;
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AppView,
    atproto::{NSID_POST, NSID_REPLY, direct_writes},
    error::AppError,
    lexicon::{post::Post, reply::Reply},
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NewRecord {
    repo: String,
    rkey: String,
    value: Value,
    signing_key: String,
    root: Value,
}

pub(crate) async fn create(
    State(state): State<AppView>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Json(new_record): Json<NewRecord>,
) -> Result<impl IntoResponse, AppError> {
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
        &new_record.root,
    )
    .await?;

    debug!("pds: {}", result);

    let uri = result
        .get("uri")
        .and_then(|uri| uri.as_str())
        .ok_or_eyre("create_record error: no uri")?;

    let cid = result
        .get("cid")
        .and_then(|cid| cid.as_str())
        .ok_or_eyre("create_record error: no cid")?;

    if let Some(Some(record_type)) = new_record.value.get("$type").map(|t| t.as_str()) {
        match record_type {
            NSID_POST => {
                Post::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
            }
            NSID_REPLY => {
                Reply::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
            }
            _ => {}
        }
    }

    Ok(ok(result))
}

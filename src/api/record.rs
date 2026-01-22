use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok, ok_simple,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

use crate::{
    AppView,
    atproto::{NSID_COMMENT, NSID_LIKE, NSID_POST, NSID_REPLY, direct_writes},
    error::AppError,
    lexicon::{
        administrator::Administrator,
        comment::Comment,
        like::Like,
        post::Post,
        reply::Reply,
        section::{Section, SectionRow},
        whitelist::Whitelist,
    },
};

#[derive(Debug, Default, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct NewRecord {
    repo: String,
    rkey: String,
    value: Value,
    signing_key: String,
    ckb_addr: String,
    root: Value,
}

#[utoipa::path(post, path = "/api/record/create")]
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
    if !Whitelist::select_by_did(&state.db, &new_record.repo).await {
        match record_type {
            NSID_POST | NSID_REPLY | NSID_COMMENT => {
                return Err(eyre!("Operation is not allowed!").into());
            }
            _ => {}
        }
    }

    if record_type == NSID_POST {
        let section_id = new_record.value["section_id"]
            .as_str()
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_eyre("error in section_id")?;
        let section: SectionRow = Section::select_by_id(&state.db, section_id)
            .await
            .map_err(|e| eyre!("error in section_id: {e}"))?;

        let is_announcement = new_record.value["is_announcement"]
            .as_bool()
            .unwrap_or(false);
        let is_top = new_record.value["is_top"].as_bool().unwrap_or(false);

        let admins = Administrator::all_did(&state.db).await;

        if (section.permission > 0 || is_announcement || is_top)
            && section.owner != Some(new_record.repo.clone())
            && !admins.contains(&new_record.repo)
        {
            return Err(eyre!("Operation is not allowed!").into());
        }
    }

    let result = direct_writes(
        &state.pds,
        auth.token(),
        &new_record.repo,
        &json!([{
            "$type": "fans.web5.ckb.directWrites#create",
            "collection": new_record.value["$type"],
            "rkey": new_record.rkey,
            "value": new_record.value
        }]),
        &new_record.signing_key,
        &new_record.ckb_addr,
        &new_record.root,
    )
    .await
    .map_err(|e| AppError::RpcFailed(e.to_string()))?;
    let uri = result
        .pointer("/results/0/uri")
        .and_then(|uri| uri.as_str())
        .ok_or(AppError::RpcFailed(result.to_string()))?;
    let cid = result
        .pointer("/results/0/cid")
        .and_then(|cid| cid.as_str())
        .ok_or(AppError::RpcFailed(result.to_string()))?;
    match record_type {
        NSID_POST => {
            Post::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
        }
        NSID_COMMENT => {
            Comment::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
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

#[utoipa::path(post, path = "/api/record/update")]
pub(crate) async fn update(
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
    if !Whitelist::select_by_did(&state.db, &new_record.repo).await {
        match record_type {
            NSID_POST | NSID_REPLY | NSID_COMMENT => {
                return Err(eyre!("Operation is not allowed!").into());
            }
            _ => {}
        }
    }

    if record_type == NSID_POST {
        let section_id = new_record.value["section_id"]
            .as_str()
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_eyre("error in section_id")?;
        let section: SectionRow = Section::select_by_id(&state.db, section_id)
            .await
            .map_err(|e| eyre!("error in section_id: {e}"))?;

        let admins = Administrator::all_did(&state.db).await;

        if section.permission > 0
            && section.owner != Some(new_record.repo.clone())
            && !admins.contains(&new_record.repo)
        {
            return Err(eyre!("Operation is not allowed!").into());
        }
    }

    let result = direct_writes(
        &state.pds,
        auth.token(),
        &new_record.repo,
        &json!([{
            "$type": "fans.web5.ckb.directWrites#update",
            "collection": new_record.value["$type"],
            "rkey": new_record.rkey,
            "value": new_record.value
        }]),
        &new_record.signing_key,
        &new_record.ckb_addr,
        &new_record.root,
    )
    .await
    .map_err(|e| AppError::RpcFailed(e.to_string()))?;
    let uri = result
        .pointer("/results/0/uri")
        .and_then(|uri| uri.as_str())
        .ok_or(AppError::RpcFailed(result.to_string()))?;
    let cid = result
        .pointer("/results/0/cid")
        .and_then(|cid| cid.as_str())
        .ok_or(AppError::RpcFailed(result.to_string()))?;
    match record_type {
        NSID_POST => {
            Post::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
        }
        NSID_COMMENT => {
            Comment::insert(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
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

#[utoipa::path(post, path = "/api/record/delete")]
pub(crate) async fn delete(
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
    if !Whitelist::select_by_did(&state.db, &new_record.repo).await {
        match record_type {
            NSID_POST | NSID_REPLY | NSID_COMMENT => {
                return Err(eyre!("Operation is not allowed!").into());
            }
            _ => {}
        }
    }

    let uri = format!(
        "at://{}/{}/{}",
        new_record.repo, record_type, new_record.rkey
    );
    Post::delete(&state.db, &uri).await?;
    direct_writes(
        &state.pds,
        auth.token(),
        &new_record.repo,
        &json!([{
            "$type": "fans.web5.ckb.directWrites#delete",
            "collection": new_record.value["$type"],
            "rkey": new_record.rkey
        }]),
        &new_record.signing_key,
        &new_record.ckb_addr,
        &new_record.root,
    )
    .await
    .map_err(|e| AppError::RpcFailed(e.to_string()))?;

    Ok(ok_simple())
}

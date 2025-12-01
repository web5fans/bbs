use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use sea_query::{Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

use crate::{
    AppView,
    atproto::{NSID_COMMENT, NSID_LIKE, NSID_POST, NSID_REPLY, direct_writes},
    error::AppError,
    lexicon::{
        comment::Comment,
        like::Like,
        post::Post,
        reply::Reply,
        section::{Section, SectionRowMini},
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
        let (sql, values) = sea_query::Query::select()
            .columns([
                Section::Id,
                Section::Name,
                Section::Owner,
                Section::Administrators,
                Section::Permission,
            ])
            .from(Section::Table)
            .and_where(Expr::col((Section::Table, Section::Id)).eq(section_id))
            .build_sqlx(PostgresQueryBuilder);
        let section: SectionRowMini = sqlx::query_as_with(&sql, values.clone())
            .fetch_one(&state.db)
            .await
            .map_err(|e| eyre!("error in section_id: {e}"))?;

        if section.permission > 0 && section.owner != Some(new_record.repo.clone()) {
            if let Some(administrators) = section.administrators {
                if !administrators.contains(&new_record.repo) {
                    return Err(eyre!("Operation is not allowed!").into());
                }
            } else {
                return Err(eyre!("Operation is not allowed!").into());
            }
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

    match record_type {
        NSID_POST | NSID_REPLY | NSID_COMMENT | NSID_LIKE => {
            let section_id = new_record.value["section_id"]
                .as_str()
                .and_then(|s| s.parse::<i32>().ok())
                .ok_or_eyre("error in section_id")?;
            let (sql, values) = sea_query::Query::select()
                .columns([
                    Section::Id,
                    Section::Name,
                    Section::Owner,
                    Section::Administrators,
                    Section::Permission,
                ])
                .from(Section::Table)
                .and_where(Expr::col((Section::Table, Section::Id)).eq(section_id))
                .build_sqlx(PostgresQueryBuilder);
            let section: SectionRowMini = sqlx::query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| eyre!("error in section_id: {e}"))?;

            if section.permission > 0 && section.owner != Some(new_record.repo.clone()) {
                if let Some(administrators) = section.administrators {
                    if !administrators.contains(&new_record.repo) {
                        return Err(eyre!("Operation is not allowed!").into());
                    }
                } else {
                    return Err(eyre!("Operation is not allowed!").into());
                }
            }
        }
        _ => {}
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

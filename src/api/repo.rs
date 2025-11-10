use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use serde::Deserialize;
use serde_json::{Value, json};
use utoipa::IntoParams;
use validator::Validate;

use crate::{AppView, api::build_author, atproto::index_query, error::AppError};

#[derive(Debug, Default, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub struct ProfileQuery {
    pub repo: String,
}

#[utoipa::path(get, path = "/api/repo/profile", params(ProfileQuery))]
pub(crate) async fn profile(
    State(state): State<AppView>,
    Query(query): Query<ProfileQuery>,
) -> Result<impl IntoResponse, AppError> {
    let mut author = build_author(&state, &query.repo).await;
    if state.whitelist.is_empty() || state.whitelist.contains(&query.repo) {
        author["highlight"] = Value::String("beta".to_owned());
    }

    Ok(ok(author))
}

#[utoipa::path(get, path = "/api/repo/login_info", params(ProfileQuery))]
pub(crate) async fn login_info(
    State(state): State<AppView>,
    Query(query): Query<ProfileQuery>,
) -> Result<impl IntoResponse, AppError> {
    let first = index_query(&state.pds, &query.repo, "firstItem")
        .await
        .map_err(|e| AppError::RpcFailed(e.to_string()))?;
    let first = first
        .pointer("/result/result")
        .cloned()
        .and_then(|i| i.as_u64())
        .ok_or(AppError::RpcFailed(first.to_string()))?;
    let second = index_query(&state.pds, &query.repo, "secondItem")
        .await
        .map_err(|e| AppError::RpcFailed(e.to_string()))?;
    let second = second
        .pointer("/result/result")
        .cloned()
        .and_then(|i| i.as_u64())
        .ok_or(AppError::RpcFailed(second.to_string()))?;
    let third = index_query(&state.pds, &query.repo, "thirdItem")
        .await
        .map_err(|e| AppError::RpcFailed(e.to_string()))?;
    let third = third
        .pointer("/result/result")
        .cloned()
        .and_then(|i| i.as_u64())
        .ok_or(AppError::RpcFailed(third.to_string()))?;

    Ok(ok(json!({
        "firstItem": first,
        "secondItem": second,
        "thirdItem": third,
    })))
}

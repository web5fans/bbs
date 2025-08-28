use color_eyre::eyre::OptionExt;
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use serde_json::{Value, json};

use crate::{AppView, api::build_author, atproto::index_query, error::AppError};

pub(crate) async fn profile(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let repo: String = query
        .get("repo")
        .and_then(|repo| repo.as_str())
        .ok_or_eyre("repo not be null")?
        .to_string();
    let mut author = build_author(&state, &repo).await;
    if state.whitelist.contains(&repo) {
        author["highlight"] = Value::String("beta".to_owned());
    }

    Ok(ok(author))
}

pub(crate) async fn login_info(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let repo: String = query
        .get("repo")
        .and_then(|repo| repo.as_str())
        .ok_or_eyre("repo not be null")?
        .to_string();
    let first = index_query(&state.pds, &repo, "firstItem")
        .await?
        .pointer("/result/result")
        .cloned()
        .and_then(|i| i.as_u64())
        .ok_or_eyre("index_query error: no result")?;
    let second = index_query(&state.pds, &repo, "secondItem")
        .await?
        .pointer("/result/result")
        .cloned()
        .and_then(|i| i.as_u64())
        .ok_or_eyre("index_query error: no result")?;
    let third = index_query(&state.pds, &repo, "thirdItem")
        .await?
        .pointer("/result/result")
        .cloned()
        .and_then(|i| i.as_u64())
        .ok_or_eyre("index_query error: no result")?;

    Ok(ok(json!({
        "firstItem": first,
        "secondItem": second,
        "thirdItem": third,
    })))
}

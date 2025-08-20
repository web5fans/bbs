use color_eyre::eyre::OptionExt;
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use serde_json::Value;

use crate::{AppView, api::build_author, error::AppError};

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

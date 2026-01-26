use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::json;
use sqlx::query_as_with;
use utoipa::IntoParams;
use validator::Validate;

use crate::{AppView, error::AppError, lexicon::whitelist::Whitelist};

#[derive(Debug, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub struct WhitelistQuery {
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for WhitelistQuery {
    fn default() -> Self {
        Self {
            page: 1,
            per_page: 20,
        }
    }
}

#[utoipa::path(get, path = "/api/whitelist", params(WhitelistQuery))]
pub(crate) async fn list(
    State(state): State<AppView>,
    Query(query): Query<WhitelistQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = sea_query::Query::select()
        .columns([Whitelist::Did])
        .from(Whitelist::Table)
        .order_by(Whitelist::Did, Order::Asc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<(String,)> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let views: Vec<String> = rows.iter().map(|row| row.0.clone()).collect();

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Whitelist::Table, Whitelist::Did)).count())
        .from(Whitelist::Table)
        .build_sqlx(PostgresQueryBuilder);

    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or_default();

    Ok(ok(json!({
        "comments": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}

use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::json;
use sqlx::query_as_with;
use validator::Validate;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    error::AppError,
    lexicon::reply::{Reply, ReplyRow, ReplyView},
};

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct ReplyQuery {
    pub root: String,
    pub parent: String,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for ReplyQuery {
    fn default() -> Self {
        Self {
            root: String::new(),
            parent: String::new(),
            page: 1,
            per_page: 20,
        }
    }
}

pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<ReplyQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::Validate(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Reply::Table, Reply::Uri),
            (Reply::Table, Reply::Cid),
            (Reply::Table, Reply::Repo),
            (Reply::Table, Reply::Root),
            (Reply::Table, Reply::Parent),
            (Reply::Table, Reply::Text),
            (Reply::Table, Reply::Updated),
            (Reply::Table, Reply::Created),
        ])
        .from(Reply::Table)
        .and_where(Expr::col((Reply::Table, Reply::Root)).eq(&query.root))
        .and_where(Expr::col((Reply::Table, Reply::Parent)).eq(&query.parent))
        .order_by(Reply::Created, Order::Desc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<ReplyRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let identity = get_record(&state.pds, &row.repo, NSID_PROFILE, "self")
            .await
            .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
            .unwrap_or(json!({
                "did": row.repo
            }));
        views.push(ReplyView {
            uri: row.uri,
            cid: row.cid,
            actior: identity,
            root: row.root,
            parent: row.parent,
            text: row.text,
            updated: row.updated,
            created: row.created,
        });
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Reply::Table, Reply::Uri)).count())
        .from(Reply::Table)
        .and_where(Expr::col((Reply::Table, Reply::Root)).eq(query.root))
        .and_where(Expr::col((Reply::Table, Reply::Parent)).eq(query.parent))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(json!({
        "replies": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}

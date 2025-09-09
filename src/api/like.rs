use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use sea_query::{BinOper, Expr, ExprTrait, Func, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::query_as_with;
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    error::AppError,
    lexicon::like::{Like, LikeRow, LikeView},
};

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct LikeQuery {
    pub repo: Option<String>,
    pub to: Option<String>,
    pub cursor: Option<String>,
    pub limit: u64,
}

impl Default for LikeQuery {
    fn default() -> Self {
        Self {
            repo: None,
            to: None,
            cursor: Default::default(),
            limit: 30,
        }
    }
}

pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<LikeQuery>,
) -> Result<impl IntoResponse, AppError> {
    let result = list_like(&state, query).await?;
    Ok(ok(result))
}

pub(crate) async fn list_like(state: &AppView, query: LikeQuery) -> Result<Value, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Like::Table, Like::Uri),
            (Like::Table, Like::Cid),
            (Like::Table, Like::Repo),
            (Like::Table, Like::To),
            (Like::Table, Like::Updated),
            (Like::Table, Like::Created),
        ])
        .from(Like::Table)
        .and_where_option(
            query
                .repo
                .map(|p| Expr::col((Like::Table, Like::Repo)).eq(&p)),
        )
        .and_where_option(query.to.map(|t| Expr::col((Like::Table, Like::To)).eq(&t)))
        .and_where_option(
            query
                .cursor
                .and_then(|cursor| cursor.parse::<i64>().ok())
                .map(|cursor| {
                    Expr::col((Like::Table, Like::Created)).binary(
                        BinOper::GreaterThan,
                        Func::cust(ToTimestamp).args([Expr::val(cursor)]),
                    )
                }),
        )
        .order_by(Like::Created, Order::Asc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<LikeRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        views.push(LikeView {
            uri: row.uri,
            cid: row.cid,
            author: build_author(state, &row.repo).await,
            to: row.to,
            updated: row.updated,
            created: row.created,
        });
    }

    let cursor = views.last().map(|r| r.created.timestamp());
    let result = if let Some(cursor) = cursor {
        json!({
            "cursor": cursor.to_string(),
            "likes": views
        })
    } else {
        json!({
            "likes": views
        })
    };

    Ok(result)
}

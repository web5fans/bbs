use color_eyre::eyre::eyre;
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
    api::{build_author, reply::ReplyQuery},
    error::AppError,
    lexicon::comment::{Comment, CommentRow, CommentView},
};

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct CommentQuery {
    pub post: String,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for CommentQuery {
    fn default() -> Self {
        Self {
            post: String::new(),
            page: 1,
            per_page: 20,
        }
    }
}

pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<CommentQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Comment::Table, Comment::Uri),
            (Comment::Table, Comment::Cid),
            (Comment::Table, Comment::Repo),
            (Comment::Table, Comment::Post),
            (Comment::Table, Comment::Text),
            (Comment::Table, Comment::Updated),
            (Comment::Table, Comment::Created),
        ])
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"comment\".\"uri\") as like_count"))
        .from(Comment::Table)
        .and_where(Expr::col((Comment::Table, Comment::Post)).eq(&query.post))
        .order_by(Comment::Created, Order::Asc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<CommentRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let replies = crate::api::reply::list_reply(
            &state,
            ReplyQuery {
                post: None,
                comment: row.uri.to_string(),
                to: None,
                cursor: None,
                limit: 2,
            },
        )
        .await
        .unwrap_or(json!({}));
        views.push(CommentView {
            uri: row.uri,
            cid: row.cid,
            author: build_author(&state, &row.repo).await,
            post: row.post,
            text: row.text,
            updated: row.updated,
            created: row.created,
            like_count: row.like_count.to_string(),
            replies,
        });
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Comment::Table, Comment::Uri)).count())
        .from(Comment::Table)
        .and_where(Expr::col((Comment::Table, Comment::Post)).eq(query.post))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(json!({
        "comments": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}

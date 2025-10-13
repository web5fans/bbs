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
    lexicon::{
        comment::{Comment, CommentRow, CommentView},
        section::Section,
    },
};

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct CommentQuery {
    pub post: String,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
    pub viewer: Option<String>,
}

impl Default for CommentQuery {
    fn default() -> Self {
        Self {
            post: String::new(),
            page: 1,
            per_page: 20,
            viewer: None,
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
            (Comment::Table, Comment::SectionId),
            (Comment::Table, Comment::Post),
            (Comment::Table, Comment::Text),
            (Comment::Table, Comment::IsDisabled),
            (Comment::Table, Comment::ReasonsForDisabled),
            (Comment::Table, Comment::Updated),
            (Comment::Table, Comment::Created),
        ])
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"comment\".\"uri\") as like_count"))
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"comment\" = \"comment\".\"uri\") as reply_count"))
        .expr(if let Some(viewer) = &query.viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"comment\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Comment::Table)
        .and_where(Expr::col((Comment::Table, Comment::Post)).eq(&query.post))
        .order_by(Comment::Created, Order::Asc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<CommentRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let sections = Section::all(&state.db).await?;
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
                viewer: query.viewer.clone(),
            },
        )
        .await
        .unwrap_or(json!({}));
        let author = build_author(&state, &row.repo).await;
        let can_see = if let Some(viewer) = &query.viewer {
            &row.repo == viewer
                || sections.get(&row.section_id).is_some_and(|section| {
                    section
                        .administrators
                        .as_ref()
                        .is_some_and(|admins| admins.contains(viewer))
                        || (section.owner.as_ref() == Some(viewer))
                })
        } else {
            false
        };

        views.push(CommentView::build(row, can_see, author, replies));
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Comment::Table, Comment::Uri)).count())
        .from(Comment::Table)
        .and_where(Expr::col((Comment::Table, Comment::Post)).eq(query.post))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

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

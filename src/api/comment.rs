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
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    api::{build_author, reply::ReplyQuery},
    atproto::NSID_COMMENT,
    error::AppError,
    lexicon::{
        administrator::Administrator,
        comment::{Comment, CommentRow, CommentView},
        section::Section,
    },
    micro_pay,
};

#[derive(Debug, Validate, Deserialize, ToSchema)]
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

#[utoipa::path(post, path = "/api/comment/list")]
pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<CommentQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = Comment::build_select(query.viewer.clone())
        .and_where(Expr::col((Comment::Table, Comment::Post)).eq(&query.post))
        .order_by(Comment::Created, Order::Asc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<CommentRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let sections = Section::all(&state.db).await?;
    let admins = Administrator::all_did(&state.db).await;
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
        let display = if let Some(viewer) = &query.viewer {
            &row.repo == viewer
                || sections
                    .get(&row.section_id)
                    .is_some_and(|section| section.owner.as_ref() == Some(viewer))
                || admins.contains(viewer)
        } else {
            false
        };
        if !row.is_disabled || display {
            let tip_count = micro_pay::payment_completed_total(
                &state.pay_url,
                &format!("{}/{}", NSID_COMMENT, row.uri),
            )
            .await
            .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
            .unwrap_or(0);
            views.push(CommentView::build(
                row,
                author,
                replies,
                tip_count.to_string(),
            ));
        }
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Comment::Table, Comment::Uri)).count())
        .from(Comment::Table)
        .and_where(Expr::col((Comment::Table, Comment::Post)).eq(query.post))
        .build_sqlx(PostgresQueryBuilder);

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

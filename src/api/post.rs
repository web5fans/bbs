use std::{collections::HashMap, sync::Arc};

use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{
        Json,
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use sea_query::{BinOper, Expr, ExprTrait, Func, IntoColumnRef, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::json;
use sqlx::{Executor, query_as_with, query_with};
use tokio::sync::RwLock;
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    atproto::NSID_POST,
    error::AppError,
    lexicon::{
        administrator::Administrator,
        comment::{Comment, CommentRow},
        post::{Post, PostDraftRow, PostDraftView, PostRepliedView, PostRow, PostView},
        section::Section,
    },
    micro_pay,
};

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct PostQuery {
    pub section_id: Option<String>,
    pub is_announcement: bool,
    pub cursor: Option<String>,
    pub limit: u64,
    pub q: Option<String>,
    pub repo: Option<String>,
    pub viewer: Option<String>,
}

impl Default for PostQuery {
    fn default() -> Self {
        Self {
            section_id: Default::default(),
            is_announcement: false,
            cursor: Default::default(),
            limit: 20,
            q: Default::default(),
            repo: Default::default(),
            viewer: Default::default(),
        }
    }
}

#[utoipa::path(post, path = "/api/post/list")]
pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<PostQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = Post::build_select(query.viewer.clone())
        .and_where(Expr::col((Post::Table, Post::IsAnnouncement)).eq(query.is_announcement))
        .and_where_option(
            query
                .repo
                .map(|repo| Expr::col((Post::Table, Post::Repo)).eq(repo)),
        )
        .and_where(
            if let Some(section) = query.section_id.and_then(|id| id.parse::<i32>().ok()) {
                Expr::col((Post::Table, Post::SectionId)).eq(section)
            } else {
                Expr::col((Post::Table, Post::SectionId)).binary(BinOper::NotEqual, 0)
            },
        )
        .and_where_option(
            query
                .cursor
                .and_then(|cursor| cursor.parse::<i64>().ok())
                .map(|cursor| {
                    Expr::col((Post::Table, Post::Updated))
                        .binary(
                            BinOper::SmallerThan,
                            Func::cust(ToTimestamp).args([Expr::val(cursor)]),
                        )
                        .and(Expr::col((Post::Table, Post::IsTop)).eq(false))
                }),
        )
        .and_where_option(query.q.map(|q| {
            (Post::Table, Post::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .and_where(if let Some(viewer) = &query.viewer {
            Expr::col((Post::Table, Post::IsDisabled))
                .eq(false)
                .or(Expr::col((Post::Table, Post::Repo)).eq(viewer))
                .or(Expr::col((Section::Table, Section::Owner)).eq(viewer))
                .or(Expr::cust(format!(
                    "(select count(did) from administrator where did = '{viewer}') > 0"
                )))
        } else {
            Expr::col((Post::Table, Post::IsDisabled)).eq(false)
        })
        .order_by_columns([
            ((Post::Table, Post::IsTop), Order::Desc),
            ((Post::Table, Post::Updated), Order::Desc),
        ])
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let views = Arc::new(RwLock::new(vec![]));
    let mut handles = vec![];
    for row in rows {
        let state = state.clone();
        let views = views.clone();
        handles.push(tokio::spawn(async move {
            let author = build_author(&state, &row.repo).await;
            let tip_count = micro_pay::payment_completed_total(
                &state.pay_url,
                &format!("{}/{}", NSID_POST, row.uri),
            )
            .await
            .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
            .unwrap_or(0);
            views
                .write()
                .await
                .push(PostView::build(row.clone(), author, tip_count.to_string()));
        }));
    }
    for handle in handles {
        handle.await?;
    }

    let mut views = views.read().await.clone();

    views.sort_by(|a, b| b.updated.cmp(&a.updated));
    views.sort_by(|a, b| b.is_top.cmp(&a.is_top));

    let cursor = views.last().map(|r| r.updated.timestamp());
    let result = if let Some(cursor) = cursor {
        json!({
            "cursor": cursor.to_string(),
            "posts": views
        })
    } else {
        json!({
            "posts": views
        })
    };
    Ok(ok(result))
}

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct PostPageQuery {
    pub section_id: Option<String>,
    pub is_announcement: bool,
    pub is_disabled: bool,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
    pub q: Option<String>,
    pub repo: Option<String>,
    pub viewer: Option<String>,
}

impl Default for PostPageQuery {
    fn default() -> Self {
        Self {
            section_id: Default::default(),
            is_announcement: false,
            is_disabled: false,
            page: 1,
            per_page: 20,
            q: Default::default(),
            repo: Default::default(),
            viewer: Default::default(),
        }
    }
}

#[utoipa::path(post, path = "/api/post/page")]
pub(crate) async fn page(
    State(state): State<AppView>,
    Json(query): Json<PostPageQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = Post::build_select(query.viewer.clone())
        .and_where(Expr::col((Post::Table, Post::IsAnnouncement)).eq(query.is_announcement))
        .and_where(Expr::col((Post::Table, Post::IsDisabled)).eq(query.is_disabled))
        .and_where_option(
            query
                .repo
                .as_ref()
                .map(|repo| Expr::col((Post::Table, Post::Repo)).eq(repo)),
        )
        .and_where_option(
            query
                .section_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok())
                .map(|section| Expr::col((Post::Table, Post::SectionId)).eq(section)),
        )
        .and_where_option(query.q.as_ref().map(|q| {
            (Post::Table, Post::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .order_by_columns([((Post::Table, Post::Updated), Order::Desc)])
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let views = Arc::new(RwLock::new(vec![]));
    let mut handles = vec![];
    for row in rows {
        let state = state.clone();
        let views = views.clone();
        handles.push(tokio::spawn(async move {
            let author = build_author(&state, &row.repo).await;
            let tip_count = micro_pay::payment_completed_total(
                &state.pay_url,
                &format!("{}/{}", NSID_POST, row.uri),
            )
            .await
            .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
            .unwrap_or(0);
            views
                .write()
                .await
                .push(PostView::build(row.clone(), author, tip_count.to_string()));
        }));
    }
    for handle in handles {
        handle.await?;
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Post::Table, Post::Uri)).count_distinct())
        .from(Post::Table)
        .and_where(Expr::col((Post::Table, Post::IsDraft)).eq(false))
        .and_where(Expr::col((Post::Table, Post::IsAnnouncement)).eq(query.is_announcement))
        .and_where(Expr::col((Post::Table, Post::IsDisabled)).eq(query.is_disabled))
        .and_where_option(
            query
                .repo
                .map(|repo| Expr::col((Post::Table, Post::Repo)).eq(repo)),
        )
        .and_where_option(
            query
                .section_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok())
                .map(|section| Expr::col((Post::Table, Post::SectionId)).eq(section)),
        )
        .and_where_option(query.q.map(|q| {
            (Post::Table, Post::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .build_sqlx(PostgresQueryBuilder);
    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or_default();

    let mut views = views.read().await.clone();
    views.sort_by(|a, b| b.updated.cmp(&a.updated));
    views.sort_by(|a, b| b.is_top.cmp(&a.is_top));
    Ok(ok(json!({
        "posts": *views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}

#[derive(Debug, Default, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct TopQuery {
    pub section_id: String,
    pub viewer: Option<String>,
}

#[utoipa::path(post, path = "/api/post/top")]
pub(crate) async fn top(
    State(state): State<AppView>,
    Json(query): Json<TopQuery>,
) -> Result<impl IntoResponse, AppError> {
    let section_id: i32 = query.section_id.parse()?;

    let (sql, values) = Post::build_select(query.viewer.clone())
        .and_where(Expr::col((Post::Table, Post::SectionId)).eq(section_id))
        .and_where(Expr::col((Post::Table, Post::IsAnnouncement)).eq(true))
        .and_where(Expr::col((Post::Table, Post::IsDisabled)).eq(false))
        .order_by_columns([
            ((Post::Table, Post::IsTop), Order::Desc),
            ((Post::Table, Post::Created), Order::Desc),
        ])
        .limit(10)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let sections = Section::all(&state.db).await?;

    let mut views = vec![];
    for row in rows {
        let author = build_author(&state, &row.repo).await;
        let display = if let Some(viewer) = &query.viewer {
            &row.repo == viewer
                || sections
                    .get(&row.section_id)
                    .is_some_and(|section| section.owner.as_ref() == Some(viewer))
        } else {
            false
        };

        if !row.is_disabled || display {
            let tip_count = micro_pay::payment_completed_total(
                &state.pay_url,
                &format!("{}/{}", NSID_POST, row.uri),
            )
            .await
            .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
            .unwrap_or(0);
            views.push(PostView::build(row, author, tip_count.to_string()));
        }
    }
    Ok(ok(json!({
        "posts": views
    })))
}

#[derive(Debug, Default, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub(crate) struct DetailQuery {
    pub uri: String,
    pub viewer: Option<String>,
}

#[utoipa::path(get, path = "/api/post/detail", params(DetailQuery))]
pub(crate) async fn detail(
    State(state): State<AppView>,
    Query(query): Query<DetailQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uri = query.uri;
    let viewer = query.viewer;

    let (sql, values) = Post::build_select(viewer.clone())
        .and_where(Expr::col(Post::Uri).eq(uri))
        .build_sqlx(PostgresQueryBuilder);

    let row: PostRow = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|_| AppError::NotFound)?;

    // update visited
    let (sql, values) = sea_query::Query::update()
        .table(Post::Table)
        .values([
            (Post::VisitedCount, (row.visited_count + 1).into()),
            (Post::Visited, (chrono::Local::now()).into()),
        ])
        .and_where(Expr::col(Post::Uri).eq(&row.uri))
        .build_sqlx(PostgresQueryBuilder);
    state.db.execute(query_with(&sql, values)).await?;

    let sections = Section::all(&state.db).await?;
    let admins = Administrator::all_did(&state.db).await;
    let author = build_author(&state, &row.repo).await;
    let display = if let Some(viewer) = &viewer {
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
            &format!("{}/{}", NSID_POST, row.uri),
        )
        .await
        .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
        .unwrap_or(0);
        Ok(ok(PostView::build(row, author, tip_count.to_string())))
    } else {
        Err(AppError::IsDisabled(
            row.reasons_for_disabled.unwrap_or_default(),
        ))
    }
}

#[utoipa::path(post, path = "/api/post/commented")]
pub(crate) async fn commented(
    State(state): State<AppView>,
    Json(query): Json<PostQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = Comment::build_select(query.viewer.clone())
        .and_where(Expr::col((Comment::Table, Comment::Repo)).eq(query.repo))
        .and_where_option(
            query
                .cursor
                .and_then(|cursor| cursor.parse::<i64>().ok())
                .map(|cursor| {
                    Expr::col((Comment::Table, Comment::Created)).binary(
                        BinOper::SmallerThan,
                        Func::cust(ToTimestamp).args([Expr::val(cursor)]),
                    )
                }),
        )
        .order_by(Comment::Created, Order::Desc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    let comments: Vec<CommentRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let cursor = comments.last().map(|r| r.created.timestamp());
    let roots = comments
        .iter()
        .map(|r| r.post.clone())
        .collect::<Vec<String>>();

    let (sql, values) = Post::build_select(query.viewer.clone())
        .and_where(Expr::col((Post::Table, Post::Uri)).is_in(roots))
        .build_sqlx(PostgresQueryBuilder);

    let posts: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let posts = posts
        .into_iter()
        .map(|p| (p.uri.clone(), p))
        .collect::<HashMap<String, PostRow>>();

    let sections = Section::all(&state.db).await?;
    let admins = Administrator::all_did(&state.db).await;
    let mut views = vec![];
    for comment in comments {
        if let Some(post) = posts.get(&comment.post).cloned() {
            let post_author = build_author(&state, &post.repo).await;
            let post_display = if let Some(viewer) = &query.viewer {
                &post.repo == viewer
                    || sections
                        .get(&post.section_id)
                        .is_some_and(|section| section.owner.as_ref() == Some(viewer))
                    || admins.contains(viewer)
            } else {
                false
            };
            let comment_display = if let Some(viewer) = &query.viewer {
                &comment.repo == viewer
                    || sections
                        .get(&comment.section_id)
                        .is_some_and(|section| section.owner.as_ref() == Some(viewer))
                    || admins.contains(viewer)
            } else {
                false
            };
            if (!post.is_disabled || post_display) && (!comment.is_disabled || comment_display) {
                let tip_count = micro_pay::payment_completed_total(
                    &state.pay_url,
                    &format!("{}/{}", NSID_POST, post.uri),
                )
                .await
                .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
                .unwrap_or(0);
                views.push(PostRepliedView::build(
                    post,
                    post_author,
                    comment,
                    tip_count.to_string(),
                ));
            }
        }
    }
    let result = if let Some(cursor) = cursor {
        json!({
            "cursor": cursor.to_string(),
            "posts": views
        })
    } else {
        json!({
            "posts": views
        })
    };
    Ok(ok(result))
}

#[utoipa::path(post, path = "/api/post/commented_page")]
pub(crate) async fn commented_page(
    State(state): State<AppView>,
    Json(query): Json<PostPageQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = Comment::build_select(query.viewer.clone())
        .and_where(Expr::col((Comment::Table, Comment::IsDisabled)).eq(query.is_disabled))
        .and_where_option(
            query
                .repo
                .as_ref()
                .map(|repo| Expr::col((Comment::Table, Comment::Repo)).eq(repo)),
        )
        .and_where_option(
            query
                .section_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok())
                .map(|section| Expr::col((Comment::Table, Comment::SectionId)).eq(section)),
        )
        .and_where_option(query.q.as_ref().map(|q| {
            (Comment::Table, Comment::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .order_by(Comment::Created, Order::Desc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let comments: Vec<CommentRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let roots = comments
        .iter()
        .map(|r| r.post.clone())
        .collect::<Vec<String>>();

    let (sql, values) = Post::build_select(query.viewer.clone())
        .and_where(Expr::col((Post::Table, Post::Uri)).is_in(roots))
        .build_sqlx(PostgresQueryBuilder);

    let posts: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let posts = posts
        .into_iter()
        .map(|p| (p.uri.clone(), p))
        .collect::<HashMap<String, PostRow>>();

    let mut views = vec![];
    for comment in comments {
        if let Some(post) = posts.get(&comment.post).cloned() {
            let post_author = build_author(&state, &post.repo).await;
            let tip_count = micro_pay::payment_completed_total(
                &state.pay_url,
                &format!("{}/{}", NSID_POST, post.uri),
            )
            .await
            .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
            .unwrap_or(0);
            views.push(PostRepliedView::build(
                post,
                post_author,
                comment,
                tip_count.to_string(),
            ));
        } else {
            warn!("post not found: {}", comment.post);
        }
    }
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Comment::Table, Comment::Uri)).count_distinct())
        .from(Comment::Table)
        .and_where(Expr::col((Comment::Table, Comment::IsDisabled)).eq(query.is_disabled))
        .and_where_option(
            query
                .repo
                .as_ref()
                .map(|repo| Expr::col((Comment::Table, Comment::Repo)).eq(repo)),
        )
        .and_where_option(
            query
                .section_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok())
                .map(|section| Expr::col((Comment::Table, Comment::SectionId)).eq(section)),
        )
        .and_where_option(query.q.as_ref().map(|q| {
            (Comment::Table, Comment::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .build_sqlx(PostgresQueryBuilder);
    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or_default();

    Ok(ok(json!({
        "posts": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct DraftQuery {
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
    pub repo: String,
}

impl Default for DraftQuery {
    fn default() -> Self {
        Self {
            page: 1,
            per_page: 20,
            repo: Default::default(),
        }
    }
}

#[utoipa::path(post, path = "/api/post/list_draft")]
pub(crate) async fn list_draft(
    State(state): State<AppView>,
    Json(query): Json<DraftQuery>,
) -> Result<impl IntoResponse, AppError> {
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = Post::build_draft_select()
        .and_where(Expr::col((Post::Table, Post::Repo)).eq(&query.repo))
        .order_by(Comment::Updated, Order::Desc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<PostDraftRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let author = build_author(&state, &row.repo).await;

        views.push(PostDraftView::build(row, author));
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Post::Table, Post::Uri)).count())
        .from(Post::Table)
        .and_where(Expr::col((Post::Table, Post::Repo)).eq(&query.repo))
        .and_where(Expr::col((Post::Table, Post::IsDraft)).eq(true))
        .build_sqlx(PostgresQueryBuilder);

    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or_default();

    Ok(ok(json!({
        "posts": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}

#[utoipa::path(get, path = "/api/post/detail_draft", params(DetailQuery))]
pub(crate) async fn detail_draft(
    State(state): State<AppView>,
    Query(query): Query<DetailQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uri = query.uri;

    let (sql, values) = Post::build_draft_select()
        .and_where(Expr::col(Post::Uri).eq(uri))
        .build_sqlx(PostgresQueryBuilder);

    let row: PostDraftRow = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|_| AppError::NotFound)?;

    let author = build_author(&state, &row.repo).await;

    Ok(ok(PostDraftView::build(row, author)))
}

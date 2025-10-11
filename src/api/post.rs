use std::collections::HashMap;

use chrono::{DateTime, Local};
use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{
        Json,
        extract::{Query, State},
        response::IntoResponse,
    },
    ok, ok_simple,
};
use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use sea_query::{BinOper, Expr, ExprTrait, Func, IntoColumnRef, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Executor, query_as_with, query_with};
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    error::AppError,
    indexer::did_document,
    lexicon::{
        comment::Comment,
        post::{Post, PostRepliedView, PostRow, PostView},
        section::{Section, SectionRowSample},
    },
};

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct PostQuery {
    pub section_id: Option<String>,
    pub is_announcement: Option<bool>,
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
            is_announcement: Default::default(),
            cursor: Default::default(),
            limit: 20,
            q: Default::default(),
            repo: Default::default(),
            viewer: Default::default(),
        }
    }
}

pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<PostQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = Post::build_select(query.viewer)
        .and_where_option(query.is_announcement.map(|is_announcement| {
            Expr::col((Post::Table, Post::IsAnnouncement)).eq(is_announcement)
        }))
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
                    Expr::col((Post::Table, Post::Updated)).binary(
                        BinOper::SmallerThan,
                        Func::cust(ToTimestamp).args([Expr::val(cursor)]),
                    )
                }),
        )
        .and_where_option(query.q.map(|q| {
            (Post::Table, Post::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .order_by_columns([
            ((Post::Table, Post::IsTop), Order::Desc),
            ((Post::Table, Post::Updated), Order::Desc),
        ])
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let author = build_author(&state, &row.repo).await;
        views.push(PostView::build(row, author));
    }
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

#[derive(Debug, Default, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct TopQuery {
    pub section_id: String,
    pub viewer: Option<String>,
}

pub(crate) async fn top(
    State(state): State<AppView>,
    Json(query): Json<TopQuery>,
) -> Result<impl IntoResponse, AppError> {
    let section_id: i32 = query.section_id.parse()?;

    let (sql, values) = sea_query::Query::select()
        .columns([Section::Id, Section::Administrators])
        .from(Section::Table)
        .and_where(Expr::col((Section::Table, Section::Id)).eq(section_id))
        .build_sqlx(PostgresQueryBuilder);
    let section: (i32, Option<Vec<String>>) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let administrators = if let Some(administrators) = section.1 {
        administrators
    } else {
        return Ok(ok(json!({
            "posts": []
        })));
    };

    if administrators.is_empty() {
        return Ok(ok(json!({
            "posts": []
        })));
    };

    let (sql, values) = Post::build_select(query.viewer)
        .and_where(Expr::col((Post::Table, Post::SectionId)).eq(section_id))
        .and_where(Expr::col((Post::Table, Post::Repo)).is_in(administrators))
        .order_by(Post::Created, Order::Desc)
        .limit(10)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let author = build_author(&state, &row.repo).await;
        views.push(PostView::build(row, author));
    }
    Ok(ok(json!({
        "posts": views
    })))
}

pub(crate) async fn detail(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let uri = query
        .get("uri")
        .and_then(|u| u.as_str())
        .ok_or_eyre("uri not be null")?;
    let viewer = query
        .get("viewer")
        .and_then(|u| u.as_str())
        .map(|s| s.to_string());

    let (sql, values) = Post::build_select(viewer)
        .and_where(Expr::col(Post::Uri).eq(uri))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let row: PostRow = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    // update visited
    let (sql, values) = sea_query::Query::update()
        .table(Post::Table)
        .values([
            (Post::VisitedCount, (row.visited_count + 1).into()),
            (Post::Visited, (chrono::Local::now()).into()),
        ])
        .and_where(Expr::col(Post::Uri).eq(&row.uri))
        .build_sqlx(PostgresQueryBuilder);
    debug!("update exec sql: {sql}");
    state.db.execute(query_with(&sql, values)).await?;

    let author = build_author(&state, &row.repo).await;
    let view = PostView::build(row, author);

    Ok(ok(view))
}

pub(crate) async fn commented(
    State(state): State<AppView>,
    Json(query): Json<PostQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Comment::Table, Comment::Post),
            (Comment::Table, Comment::Text),
            (Comment::Table, Comment::Created),
        ])
        .from(Comment::Table)
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

    let rows: Vec<(String, String, DateTime<Local>)> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let cursor = rows.last().map(|r| r.2.timestamp());
    let roots = rows
        .into_iter()
        .map(|r| (r.0, (r.1, r.2)))
        .collect::<HashMap<String, (String, DateTime<Local>)>>();

    let (sql, values) = Post::build_select(query.viewer)
        .and_where(Expr::col((Post::Table, Post::Uri)).is_in(roots.keys()))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let comment = roots.get(&row.uri).cloned().unwrap_or_default();
        let author = build_author(&state, &row.repo).await;
        views.push(PostRepliedView::build(row, author, comment));
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

#[derive(Debug, Default, Validate, Deserialize, Serialize)]
#[serde(default)]
pub(crate) struct UpdateTagParams {
    pub uri: String,
    pub is_top: Option<bool>,
    pub is_announcement: Option<bool>,
    pub is_disabled: Option<bool>,
    pub reasons_for_disabled: Option<String>,
}

#[derive(Debug, Default, Validate, Deserialize, Serialize)]
#[serde(default)]
pub(crate) struct UpdateTagBody {
    pub params: UpdateTagParams,
    pub did: String,
    #[validate(length(equal = 57))]
    pub signing_key_did: String,
    pub signed_bytes: String,
}

pub(crate) async fn update_by_admin(
    State(state): State<AppView>,
    Json(body): Json<UpdateTagBody>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let did_doc = did_document(&state.indexer, body.did.as_str())
        .await
        .map_err(|e| {
            debug!("call indexer failed: {e}");
            AppError::ValidateFailed("get did doc failed".to_string())
        })?;

    if body.signing_key_did
        != did_doc
            .pointer("/verificationMethods/atproto")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
    {
        return Err(AppError::ValidateFailed(
            "signing_key_did not match".to_string(),
        ));
    }

    let (sql, values) = Post::build_select(None)
        .and_where(Expr::col(Post::Uri).eq(body.params.uri.clone()))
        .build_sqlx(PostgresQueryBuilder);
    debug!("sql: {sql} ({values:?})");
    let post_row: PostRow = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    let (sql, values) = Section::build_select()
        .and_where(Expr::col(Section::Id).eq(post_row.section_id))
        .build_sqlx(PostgresQueryBuilder);
    debug!("sql: {sql} ({values:?})");
    let section_row: SectionRowSample = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    if section_row.owner == Some(body.did.clone())
        || section_row
            .administrators
            .unwrap_or_default()
            .contains(&body.did)
    {
        // TODO: verify signature
        let verifying_key: VerifyingKey = body
            .signing_key_did
            .split_once("did:key:z")
            .and_then(|(_, key)| {
                let bytes = bs58::decode(key).into_vec().ok()?;
                VerifyingKey::from_sec1_bytes(&bytes[2..]).ok()
            })
            .ok_or_else(|| AppError::ValidateFailed("invalid signing_key_did".to_string()))?;
        let signature = hex::decode(&body.signed_bytes)
            .map(|bytes| Signature::from_slice(&bytes).map_err(|e| eyre!(e)))??;
        verifying_key
            .verify(&serde_ipld_dagcbor::to_vec(&body.params)?, &signature)
            .map_err(|e| {
                debug!("verify signature failed: {e}");
                AppError::ValidateFailed("verify signature failed".to_string())
            })?;

        Post::update_tag(
            &state.db,
            &body.params.uri,
            body.params.is_top,
            body.params.is_announcement,
            body.params.is_disabled,
            body.params.reasons_for_disabled,
        )
        .await?;
    } else {
        return Err(AppError::ValidateFailed(
            "only section administrator can update post tag".to_string(),
        ));
    }

    Ok(ok_simple())
}

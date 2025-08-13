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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::query_as_with;
use validator::Validate;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    error::AppError,
    lexicon::{
        post::{Post, PostRow, PostView},
        section::Section,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NewPost {
    repo: String,
    rkey: String,
    record: Value,
    signing_key: String,
    root: Value,
}

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct PostQuery {
    pub section_id: Option<i32>,
    pub cursor: Option<String>,
    pub limit: u64,
    pub q: Option<String>,
    pub repo: Option<String>,
}

impl Default for PostQuery {
    fn default() -> Self {
        Self {
            section_id: Default::default(),
            cursor: Default::default(),
            limit: 15,
            q: Default::default(),
            repo: Default::default(),
        }
    }
}

struct ToTimestamp;

impl sea_query::Iden for ToTimestamp {
    fn unquoted(&self) -> &str {
        "to_timestamp"
    }
}

pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<PostQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Post::Table, Post::Uri),
            (Post::Table, Post::Cid),
            (Post::Table, Post::Repo),
            (Post::Table, Post::Title),
            (Post::Table, Post::Text),
            (Post::Table, Post::VisitedCount),
            (Post::Table, Post::Visited),
            (Post::Table, Post::Updated),
            (Post::Table, Post::Created),
        ])
        .column((Section::Table, Section::Name))
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
        .and_where_option(query.q.map(|q| {
            (Post::Table, Post::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .and_where_option(
            query
                .repo
                .map(|repo| Expr::col((Post::Table, Post::Repo)).eq(repo)),
        )
        .and_where_option(
            query
                .section_id
                .map(|section| Expr::col((Post::Table, Post::SectionId)).eq(section)),
        )
        .and_where_option(query.cursor.map(|cursor| {
            Expr::col((Post::Table, Post::Created)).binary(
                BinOper::SmallerThan,
                Func::cust(ToTimestamp)
                    .args([Expr::val(cursor), Expr::val("YYYY-MM-DD HH24:MI:SS")]),
            )
        }))
        .order_by(Post::Created, Order::Desc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<PostRow> = query_as_with::<_, PostRow, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let identity_row = get_record(&state.pds, &row.repo, NSID_PROFILE, "self")
            .await
            .unwrap_or(json!({}));
        views.push(PostView {
            uri: row.uri,
            cid: row.cid,
            actior: identity_row.get("value").cloned().unwrap_or(json!({})),
            title: row.title,
            text: row.text,
            visited_count: row.visited_count,
            visited: row.visited,
            updated: row.updated,
            created: row.created,
            section: row.section,
        });
    }
    let cursor = views
        .last()
        .map(|r| r.created.format("%Y-%m-%d %H:%M:%S").to_string());
    let result = if let Some(cursor) = cursor {
        json!({
            "cursor": cursor,
            "posts": views
        })
    } else {
        json!({
            "posts": views
        })
    };
    Ok(ok(result))
}

pub(crate) async fn detail(
    State(state): State<AppView>,
    Query(uri): Query<String>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Post::Table, Post::Uri),
            (Post::Table, Post::Cid),
            (Post::Table, Post::Repo),
            (Post::Table, Post::Title),
            (Post::Table, Post::Text),
            (Post::Table, Post::VisitedCount),
            (Post::Table, Post::Visited),
            (Post::Table, Post::Updated),
            (Post::Table, Post::Created),
        ])
        .column((Section::Table, Section::Name))
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
        .and_where(Expr::col(Post::Uri).eq(uri))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let row: PostRow = query_as_with::<_, PostRow, _>(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let identity_row = get_record(&state.pds, &row.repo, NSID_PROFILE, "self")
        .await
        .unwrap_or(json!({}));
    let view = PostView {
        uri: row.uri,
        cid: row.cid,
        actior: identity_row.get("value").cloned().unwrap_or(json!({})),
        title: row.title,
        text: row.text,
        visited_count: row.visited_count,
        visited: row.visited,
        updated: row.updated,
        created: row.created,
        section: row.section,
    };

    Ok(ok(view))
}

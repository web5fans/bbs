use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok, ok_simple,
};
use sea_query::{BinOper, Expr, ExprTrait, Func, IntoColumnRef, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Executor, query_as_with, query_with};
use validator::Validate;

use crate::{
    AppView,
    atproto::{NSID_POST, NSID_PROFILE, direct_writes, get_record},
    error::AppError,
    lexicon::{
        post::{Post, PostRow, PostView},
        section::Section,
    },
};

#[derive(Debug, Validate, Serialize, Deserialize)]
pub(crate) struct NewPost {
    #[validate(length(min = 1))]
    repo: String,
    rkey: String,
    record: PostRecord,
    signing_key: String,
    root: Value,
}

#[derive(Debug, Validate, Serialize, Deserialize)]
pub(crate) struct PostRecord {
    section_id: u8,
    #[validate(length(max = 60))]
    title: String,
    #[validate(length(max = 10000))]
    text: String,
}

pub(crate) async fn create(
    State(state): State<AppView>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Json(post): Json<NewPost>,
) -> Result<impl IntoResponse, AppError> {
    post.validate()
        .map_err(|e| AppError::Validate(e.to_string()))?;

    let created = chrono::Local::now().naive_local();
    info!("created: {}", created);

    let result = direct_writes(
        &state.pds,
        auth.token(),
        &post.repo,
        &json!([{
            "$type": "com.atproto.web5.directWrites#create",
            "collection": NSID_POST,
            "rkey": post.rkey,
            "value": post.record
        }]),
        &post.signing_key,
        &post.root,
    )
    .await?;

    info!("pds: {}", result);

    let uri = result
        .get("uri")
        .and_then(|uri| uri.as_str())
        .ok_or_eyre("create_record error: no uri")?;

    let cid = result
        .get("cid")
        .and_then(|cid| cid.as_str())
        .ok_or_eyre("create_record error: no cid")?;

    let (sql, values) = sea_query::Query::insert()
        .into_table(Post::Table)
        .columns([
            Post::Uri,
            Post::Cid,
            Post::Repo,
            Post::SectionId,
            Post::Title,
            Post::Text,
        ])
        .values([
            uri.into(),
            cid.into(),
            post.repo.into(),
            post.record.section_id.into(),
            post.record.title.into(),
            post.record.text.into(),
        ])?
        .returning_col(Post::Uri)
        .build_sqlx(PostgresQueryBuilder);

    debug!("write_posts exec sql: {sql}");

    // execute the SQL query
    state
        .db
        .execute(query_with(&sql, values))
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok_simple())
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
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
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

    info!("sql: {sql}");

    let rows: Vec<PostRow> = query_as_with::<_, PostRow, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let identity_row = get_record(&state.pds, auth.token(), &row.repo, NSID_PROFILE, "self")
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

#[allow(dead_code)]
pub(crate) async fn detail(
    State(_state): State<AppView>,
    TypedHeader(_auth): TypedHeader<Authorization<Bearer>>,
    Json(_post): Json<NewPost>,
) -> Result<impl IntoResponse, AppError> {
    Ok(ok_simple())
}

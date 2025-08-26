use std::collections::HashMap;

use chrono::{DateTime, Local};
use color_eyre::eyre::{OptionExt, eyre};
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
use sqlx::{Executor, query_as_with, query_with};
use validator::Validate;

use crate::{
    AppView,
    api::build_author,
    error::AppError,
    lexicon::{
        post::{Post, PostRepliedView, PostRow, PostView},
        reply::Reply,
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
    pub section_id: Option<String>,
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
            limit: 20,
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
        .columns([
            (Section::Table, Section::Id),
            (Section::Table, Section::Name),
        ])
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"root\" = \"post\".\"uri\") as reply_count"))
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
        .and_where(
            if let Some(section) = query.section_id.and_then(|id| id.parse::<i32>().ok()) {
                Expr::col((Post::Table, Post::SectionId)).eq(section)
            } else {
                Expr::col((Post::Table, Post::SectionId)).binary(BinOper::NotEqual, 0)
            },
        )
        .and_where_option(query.cursor.map(|cursor| {
            Expr::col((Post::Table, Post::Created)).binary(
                BinOper::SmallerThan,
                Func::cust(ToTimestamp)
                    .args([Expr::val(cursor), Expr::val("YYYY-MM-DDTHH24:MI:SS")]),
            )
        }))
        .order_by(Post::Updated, Order::Desc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<PostRow> = query_as_with::<_, PostRow, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        views.push(PostView {
            uri: row.uri,
            cid: row.cid,
            author: build_author(&state, &row.repo).await,
            title: row.title,
            text: row.text,
            visited_count: row.visited_count.to_string(),
            visited: row.visited,
            updated: row.updated,
            created: row.created,
            section_id: row.section_id.to_string(),
            section: row.section,
            reply_count: row.reply_count.to_string(),
        });
    }
    let cursor = views.last().map(|r| r.created.to_rfc3339());
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

#[derive(Debug, Default, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct TopQuery {
    pub section_id: String,
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
        .columns([
            (Section::Table, Section::Id),
            (Section::Table, Section::Name),
        ])
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"root\" = \"post\".\"uri\") as reply_count"))
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
        .and_where(Expr::col((Post::Table, Post::SectionId)).eq(section_id))
        .and_where(Expr::col((Post::Table, Post::Repo)).is_in(administrators))
        .order_by(Post::Created, Order::Desc)
        .limit(10)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<PostRow> = query_as_with::<_, PostRow, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        views.push(PostView {
            uri: row.uri,
            cid: row.cid,
            author: build_author(&state, &row.repo).await,
            title: row.title,
            text: row.text,
            visited_count: row.visited_count.to_string(),
            visited: row.visited,
            updated: row.updated,
            created: row.created,
            section_id: row.section_id.to_string(),
            section: row.section,
            reply_count: row.reply_count.to_string(),
        });
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
        .columns([
            (Section::Table, Section::Id),
            (Section::Table, Section::Name),
        ])
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"root\" = \"post\".\"uri\") as reply_count"))
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

    let view = PostView {
        uri: row.uri,
        cid: row.cid,
        author: build_author(&state, &row.repo).await,
        title: row.title,
        text: row.text,
        visited_count: row.visited_count.to_string(),
        visited: row.visited,
        updated: row.updated,
        created: row.created,
        section_id: row.section_id.to_string(),
        section: row.section,
        reply_count: row.reply_count.to_string(),
    };

    Ok(ok(view))
}

pub(crate) async fn replied(
    State(state): State<AppView>,
    Json(query): Json<PostQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Reply::Table, Reply::Root),
            (Reply::Table, Reply::Text),
            (Reply::Table, Reply::Created),
        ])
        .from(Reply::Table)
        .and_where(Expr::col((Reply::Table, Reply::Repo)).eq(query.repo))
        .and_where_option(query.cursor.map(|cursor| {
            Expr::col((Reply::Table, Reply::Created)).binary(
                BinOper::SmallerThan,
                Func::cust(ToTimestamp)
                    .args([Expr::val(cursor), Expr::val("YYYY-MM-DDTHH24:MI:SS")]),
            )
        }))
        .order_by(Reply::Created, Order::Desc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<(String, String, DateTime<Local>)> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let cursor = rows.last().map(|r| r.2.to_rfc3339());
    let roots = rows
        .into_iter()
        .map(|r| (r.0, (r.1, r.2)))
        .collect::<HashMap<String, (String, DateTime<Local>)>>();

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
        .columns([
            (Section::Table, Section::Id),
            (Section::Table, Section::Name),
        ])
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"root\" = \"post\".\"uri\") as reply_count"))
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
        .and_where(Expr::col((Post::Table, Post::Uri)).is_in(roots.keys()))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<PostRow> = query_as_with::<_, PostRow, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let reply = roots.get(&row.uri).cloned().unwrap_or_default();
        views.push(PostRepliedView {
            reply_text: reply.0,
            reply_created: reply.1,
            uri: row.uri,
            cid: row.cid,
            author: build_author(&state, &row.repo).await,
            title: row.title,
            text: row.text,
            visited_count: row.visited_count.to_string(),
            visited: row.visited,
            updated: row.updated,
            created: row.created,
            section_id: row.section_id.to_string(),
            section: row.section,
            reply_count: row.reply_count.to_string(),
        });
    }
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

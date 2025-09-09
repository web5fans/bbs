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
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{Executor, query_as_with, query_with};
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    error::AppError,
    lexicon::{
        comment::Comment,
        post::{Post, PostRepliedView, PostRow, PostView},
        section::Section,
    },
};

#[derive(Debug, Validate, Deserialize)]
#[serde(default)]
pub(crate) struct PostQuery {
    pub section_id: Option<String>,
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
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"post\" = \"post\".\"uri\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"post\".\"uri\") as like_count"))
        .expr(if let Some(viewer) = query.viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"post\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
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
        .and_where_option(query.cursor.and_then(|cursor| cursor.parse::<i64>().ok()).map(|cursor| {
            Expr::col((Post::Table, Post::Updated)).binary(
                BinOper::SmallerThan,
                Func::cust(ToTimestamp)
                    .args([Expr::val(cursor)]),
            )
        }))
        .and_where_option(query.q.map(|q| {
            (Post::Table, Post::Text)
                .into_column_ref()
                .like(format!("%{q}%"))
        }))
        .order_by(Post::Updated, Order::Desc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
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
            comment_count: row.comment_count.to_string(),
            like_count: row.like_count.to_string(),
            liked: row.liked,
        });
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
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"post\" = \"post\".\"uri\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"post\".\"uri\") as like_count"))
        .expr(if let Some(viewer) = query.viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"post\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
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

    debug!("sql: {sql} ({values:?})");

    let rows: Vec<PostRow> = query_as_with(&sql, values.clone())
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
            comment_count: row.comment_count.to_string(),
            like_count: row.like_count.to_string(),
            liked: row.liked,
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
    let viewer = query.get("viewer").and_then(|u| u.as_str());

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
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"post\" = \"post\".\"uri\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"post\".\"uri\") as like_count"))
        .expr(if let Some(viewer) = viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"post\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
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
        comment_count: row.comment_count.to_string(),
        like_count: row.like_count.to_string(),
        liked: row.liked,
    };

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
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"post\" = \"post\".\"uri\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"post\".\"uri\") as like_count"))
        .expr(if let Some(viewer) = query.viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"post\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
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
        views.push(PostRepliedView {
            comment_text: comment.0,
            comment_created: comment.1,
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
            comment_count: row.comment_count.to_string(),
            like_count: row.like_count.to_string(),
            liked: row.liked,
        });
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

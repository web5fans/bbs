use color_eyre::eyre::OptionExt;
use sea_query::{BinOper, Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde_json::{Value, json};
use sqlx::query_as_with;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    lexicon::{like::Like, post::Post, reply::Reply},
};

pub(crate) mod comment;
pub(crate) mod like;
pub(crate) mod post;
pub(crate) mod record;
pub(crate) mod reply;
pub(crate) mod repo;
pub(crate) mod section;

pub(crate) struct ToTimestamp;

impl sea_query::Iden for ToTimestamp {
    fn unquoted(&self) -> &str {
        "to_timestamp"
    }
}

pub(crate) async fn build_author(state: &AppView, repo: &str) -> Value {
    // Get post count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Post::Table, Post::Uri)).count())
        .from(Post::Table)
        .and_where(Expr::col(Post::Repo).eq(repo))
        .and_where(Expr::col((Post::Table, Post::SectionId)).binary(BinOper::NotEqual, 0))
        .build_sqlx(PostgresQueryBuilder);
    debug!("post count exec sql: {sql}");
    let post_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get comment count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Reply::Table, Reply::Uri)).count())
        .from(Reply::Table)
        .and_where(Expr::col(Reply::Repo).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    debug!("comment count exec sql: {sql}");
    let comment_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get like count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Like::Table, Like::Uri)).count())
        .from(Reply::Table)
        .and_where(Expr::col(Like::To).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    debug!("like count exec sql: {sql}");
    let like_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get profile
    let mut author = get_record(&state.pds, repo, NSID_PROFILE, "self")
        .await
        .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
        .unwrap_or(json!({
            "did": repo
        }));
    author["did"] = Value::String(repo.to_owned());
    author["post_count"] = Value::String(post_count_row.0.to_string());
    author["comment_count"] = Value::String(comment_count_row.0.to_string());
    author["like_count"] = Value::String(like_count_row.0.to_string());
    author
}

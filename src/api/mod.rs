use color_eyre::eyre::OptionExt;
use sea_query::{Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde_json::{Value, json};
use sqlx::query_as_with;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    lexicon::{post::Post, reply::Reply},
};

pub(crate) mod post;
pub(crate) mod record;
pub(crate) mod reply;
pub(crate) mod section;

pub(crate) async fn build_author(state: &AppView, repo: &str) -> Value {
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Post::Table, Post::Uri)).count())
        .from(Post::Table)
        .and_where(Expr::col(Post::Repo).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    debug!("post count exec sql: {sql}");
    let post_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Reply::Table, Reply::Uri)).count())
        .from(Reply::Table)
        .and_where(Expr::col(Reply::Repo).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    debug!("reply count exec sql: {sql}");
    let reply_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));
    let mut author = get_record(&state.pds, repo, NSID_PROFILE, "self")
        .await
        .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
        .unwrap_or(json!({
            "did": repo
        }));
    author["did"] = Value::String(repo.to_owned());
    author["post_count"] = Value::String(post_count_row.0.to_string());
    author["reply_count"] = Value::String(reply_count_row.0.to_string());
    author
}

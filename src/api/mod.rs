use color_eyre::eyre::OptionExt;
use sea_query::{BinOper, Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde_json::{Value, json};
use sqlx::query_as_with;
use utoipa::{
    Modify, OpenApi,
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
};

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    ckb::get_ckb_addr_by_did,
    lexicon::{comment::Comment, like::Like, post::Post},
};

pub(crate) mod admin;
pub(crate) mod comment;
pub(crate) mod donate;
pub(crate) mod like;
pub(crate) mod post;
pub(crate) mod record;
pub(crate) mod reply;
pub(crate) mod repo;
pub(crate) mod section;
pub(crate) mod tip;

#[derive(OpenApi, Debug, Clone, Copy)]
#[openapi(
    modifiers(&SecurityAddon),
    paths(
        admin::update_tag,
        record::create,
        record::update,
        section::list,
        section::detail,
        post::list,
        post::top,
        post::detail,
        post::commented,
        comment::list,
        reply::list,
        repo::profile,
        repo::login_info,
        like::list,
        tip::prepare,
        tip::transfer,
        tip::list_by_for,
        tip::expense_details,
        tip::income_details,
        tip::stats,
        donate::prepare,
        donate::transfer,
    ),
    components(schemas(
        admin::UpdateTagParams,
        admin::UpdateTagBody,
        record::NewRecord,
        post::PostQuery,
        post::TopQuery,
        comment::CommentQuery,
        reply::ReplyQuery,
        like::LikeQuery,
        tip::TipParams,
        tip::TipBody,
        tip::TipsQuery,
        tip::DetailQuery,
        donate::DonateParams,
        donate::DonateBody,
    ))
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "Authorization",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("Authorization"))),
            )
        }
    }
}

pub(crate) struct ToTimestamp;

impl sea_query::Iden for ToTimestamp {
    fn unquoted(&self) -> &str {
        "to_timestamp"
    }
}

pub(crate) async fn build_author(state: &AppView, repo: &str) -> Value {
    if !repo.starts_with("did:") {
        return Value::String(repo.to_string());
    }
    // Get post count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Post::Table, Post::Uri)).count())
        .from(Post::Table)
        .and_where(Expr::col(Post::Repo).eq(repo))
        .and_where(Expr::col((Post::Table, Post::SectionId)).binary(BinOper::NotEqual, 0))
        .build_sqlx(PostgresQueryBuilder);
    let post_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get comment count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Comment::Table, Comment::Uri)).count())
        .from(Comment::Table)
        .and_where(Expr::col(Comment::Repo).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    let comment_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get like count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Like::Table, Like::Uri)).count())
        .from(Like::Table)
        .and_where(Expr::col(Like::To).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
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
    if let Ok(ckb_addr) = get_ckb_addr_by_did(&state.ckb_client, &state.ckb_net, repo).await {
        author["ckb_addr"] = Value::String(ckb_addr);
    }
    author["did"] = Value::String(repo.to_owned());
    author["post_count"] = Value::String(post_count_row.0.to_string());
    author["comment_count"] = Value::String(comment_count_row.0.to_string());
    author["like_count"] = Value::String(like_count_row.0.to_string());
    author
}

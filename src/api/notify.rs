use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok, ok_simple,
};
use sea_query::{BinOper, Expr, ExprTrait, Func, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{Executor, Pool, Postgres, query_as_with, query_with};
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    atproto::{NSID_COMMENT, NSID_POST, NSID_REPLY},
    error::AppError,
    lexicon::{
        comment::Comment,
        notify::{Notify, NotifyRow, NotifyView},
        post::Post,
        reply::Reply,
        resolve_uri,
    },
};

#[derive(Debug, Default, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub struct NotifyQuery {
    pub repo: String,
    pub n_type: Option<String>,
    pub cursor: Option<String>,
    pub limit: u64,
}

#[utoipa::path(post, path = "/api/notify/list")]
pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<NotifyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = Notify::build_select()
        .and_where(Expr::col(Notify::Receiver).eq(query.repo))
        .and_where(Expr::col(Notify::Readed).is_null())
        .and_where_option(
            query
                .n_type
                .and_then(|t| t.parse::<i64>().ok())
                .map(|t| Expr::col((Notify::Table, Notify::NType)).eq(t)),
        )
        .and_where_option(
            query
                .cursor
                .and_then(|cursor| cursor.parse::<i64>().ok())
                .map(|cursor| {
                    Expr::col((Notify::Table, Notify::Created)).binary(
                        BinOper::SmallerThan,
                        Func::cust(ToTimestamp).args([Expr::val(cursor)]),
                    )
                }),
        )
        .order_by(Notify::Created, Order::Desc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);
    let rows: Vec<NotifyRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let target = get_target(&state.db, &row.target_uri)
            .await
            .unwrap_or_default();

        views.push(NotifyView {
            id: row.id.to_string(),
            title: row.title,
            sender: build_author(&state, &row.sender).await,
            receiver: build_author(&state, &row.receiver).await,
            n_type: row.n_type.to_string(),
            target_uri: row.target_uri,
            target,
            amount: row.amount,
            readed: row.readed,
            created: row.created,
        });
    }

    let cursor = views.last().map(|r| r.created.timestamp());
    let result = if let Some(cursor) = cursor {
        json!({
            "cursor": cursor.to_string(),
            "notifies": views
        })
    } else {
        json!({
            "notifies": views
        })
    };

    Ok(ok(result))
}

async fn get_target(db: &Pool<Postgres>, uri: &str) -> Result<Value> {
    let (_did, nsid, _rkey) = resolve_uri(uri)?;

    let value = match nsid {
        NSID_POST => {
            let (sql, values) = sea_query::Query::select()
                .columns([
                    (Post::Table, Post::Title),
                    (Post::Table, Post::ReasonsForDisabled),
                ])
                .from(Post::Table)
                .and_where(Expr::col(Post::Uri).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, Option<String>) =
                query_as_with(&sql, values.clone()).fetch_one(db).await?;
            json!({
                "nsid": nsid,
                "title": row.0,
                "reasons_for_disabled": row.1,
            })
        }
        NSID_COMMENT => {
            let (sql, values) = sea_query::Query::select()
                .columns([
                    (Comment::Table, Comment::Text),
                    (Comment::Table, Comment::Post),
                    (Comment::Table, Comment::Created),
                    (Comment::Table, Comment::ReasonsForDisabled),
                ])
                .from(Comment::Table)
                .and_where(Expr::col(Comment::Uri).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String, DateTime<Local>, Option<String>) =
                query_as_with(&sql, values.clone()).fetch_one(db).await?;

            let (sql, values) = sea_query::Query::select()
                .expr(Expr::col((Comment::Table, Comment::Uri)).count_distinct())
                .from(Comment::Table)
                .and_where(Expr::col((Comment::Table, Comment::Post)).eq(&row.1))
                .and_where(
                    Expr::col((Comment::Table, Comment::Created))
                        .binary(BinOper::SmallerThan, row.2),
                )
                .build_sqlx(PostgresQueryBuilder);
            let count: (i64,) = query_as_with(&sql, values.clone()).fetch_one(db).await?;

            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::Title)])
                .from(Post::Table)
                .and_where(Expr::col(Post::Uri).eq(&row.1))
                .build_sqlx(PostgresQueryBuilder);
            let post: (String,) = query_as_with(&sql, values.clone()).fetch_one(db).await?;

            json!({
                "nsid": nsid,
                "text": row.0,
                "index": count.0 + 1,
                "reasons_for_disabled": row.3,
                "post": {
                    "title": post.0,
                    "uri": row.1
                },
            })
        }
        NSID_REPLY => {
            let (sql, values) = sea_query::Query::select()
                .columns([
                    (Reply::Table, Reply::Text),
                    (Reply::Table, Reply::Comment),
                    (Reply::Table, Reply::Created),
                    (Reply::Table, Reply::ReasonsForDisabled),
                ])
                .from(Reply::Table)
                .and_where(Expr::col(Reply::Uri).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let reply: (String, String, DateTime<Local>, Option<String>) =
                query_as_with(&sql, values.clone()).fetch_one(db).await?;

            let (sql, values) = sea_query::Query::select()
                .expr(Expr::col((Reply::Table, Reply::Uri)).count_distinct())
                .from(Reply::Table)
                .and_where(Expr::col((Reply::Table, Reply::Comment)).eq(&reply.1))
                .and_where(
                    Expr::col((Reply::Table, Reply::Created)).binary(BinOper::SmallerThan, reply.2),
                )
                .build_sqlx(PostgresQueryBuilder);
            let reply_count: (i64,) = query_as_with(&sql, values.clone()).fetch_one(db).await?;

            let (sql, values) = sea_query::Query::select()
                .columns([
                    (Comment::Table, Comment::Text),
                    (Comment::Table, Comment::Post),
                    (Comment::Table, Comment::Created),
                ])
                .from(Comment::Table)
                .and_where(Expr::col(Comment::Uri).eq(&reply.1))
                .build_sqlx(PostgresQueryBuilder);
            let comment: (String, String, DateTime<Local>) =
                query_as_with(&sql, values.clone()).fetch_one(db).await?;

            let (sql, values) = sea_query::Query::select()
                .expr(Expr::col((Comment::Table, Comment::Uri)).count_distinct())
                .from(Comment::Table)
                .and_where(Expr::col((Comment::Table, Comment::Post)).eq(&comment.1))
                .and_where(
                    Expr::col((Comment::Table, Comment::Created))
                        .binary(BinOper::SmallerThan, comment.2),
                )
                .build_sqlx(PostgresQueryBuilder);
            let comment_count: (i64,) = query_as_with(&sql, values.clone()).fetch_one(db).await?;

            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::Title)])
                .from(Post::Table)
                .and_where(Expr::col(Post::Uri).eq(&comment.1))
                .build_sqlx(PostgresQueryBuilder);
            let post: (String,) = query_as_with(&sql, values.clone()).fetch_one(db).await?;

            json!({
                "nsid": nsid,
                "text": reply.0,
                "index": reply_count.0 + 1,
                "reasons_for_disabled": reply.3,
                "comment": {
                    "uri": reply.1,
                    "text": comment.0,
                    "index": comment_count.0 + 1,
                },
                "post": {
                    "title": post.0,
                    "uri": comment.1
                },
            })
        }
        _ => return Err(eyre!("nsid not supported: {nsid}")),
    };

    Ok(value)
}

#[derive(Debug, Default, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub struct NotifyReadQuery {
    pub repo: String,
    pub target: Option<i32>,
}

#[utoipa::path(post, path = "/api/notify/read")]
pub(crate) async fn read(
    State(state): State<AppView>,
    Json(query): Json<NotifyReadQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::update()
        .table(Notify::Table)
        .values([(Notify::Readed, Expr::current_timestamp())])
        .and_where(Expr::col(Notify::Receiver).eq(query.repo))
        .and_where_option(query.target.map(|target| Expr::col(Notify::Id).eq(target)))
        .build_sqlx(PostgresQueryBuilder);

    state.db.execute(query_with(&sql, values)).await?;
    Ok(ok_simple())
}

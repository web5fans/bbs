use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok, ok_simple,
};
use sea_query::{BinOper, Expr, ExprTrait, Func, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::json;
use sqlx::{Executor, query_as_with, query_with};
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    error::AppError,
    lexicon::notify::{Notify, NotifyRow, NotifyView},
};

#[derive(Debug, Default, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub struct NotifyQuery {
    pub repo: String,
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
        views.push(NotifyView {
            id: row.id.to_string(),
            title: row.title,
            sender: build_author(&state, &row.sender).await,
            receiver: build_author(&state, &row.receiver).await,
            target_nsid: row.target_nsid,
            target_did: row.target_did,
            target: json!({}),
            readed: row.readed,
            created: row.created,
        });
    }

    Ok(ok(views))
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

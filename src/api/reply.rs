use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use sea_query::{BinOper, Expr, ExprTrait, Func, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::query_as_with;
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    api::{ToTimestamp, build_author},
    atproto::NSID_REPLY,
    error::AppError,
    lexicon::{
        administrator::Administrator,
        reply::{Reply, ReplyRow, ReplySampleRow, ReplyView},
        section::Section,
    },
    micro_pay,
};

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct ReplyQuery {
    pub post: Option<String>,
    pub comment: String,
    pub to: Option<String>,
    pub cursor: Option<String>,
    pub limit: u64,
    pub viewer: Option<String>,
}

impl Default for ReplyQuery {
    fn default() -> Self {
        Self {
            post: None,
            comment: String::new(),
            to: None,
            cursor: Default::default(),
            limit: 2,
            viewer: None,
        }
    }
}

#[utoipa::path(post, path = "/api/reply/list")]
pub(crate) async fn list(
    State(state): State<AppView>,
    Json(query): Json<ReplyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let result = list_reply(&state, query).await?;
    Ok(ok(result))
}

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct ReplyPageQuery {
    pub section_id: Option<String>,
    pub q: Option<String>,
    pub is_disabled: bool,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for ReplyPageQuery {
    fn default() -> Self {
        Self {
            section_id: Default::default(),
            is_disabled: false,
            page: 1,
            per_page: 20,
            q: Default::default(),
        }
    }
}

#[utoipa::path(post, path = "/api/reply/page")]
pub(crate) async fn page(
    State(state): State<AppView>,
    Json(query): Json<ReplyPageQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let result = page_reply(&state, query).await?;
    Ok(ok(result))
}

async fn page_reply(state: &AppView, query: ReplyPageQuery) -> Result<Value, AppError> {
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Reply::Table, Reply::Uri),
            (Reply::Table, Reply::Cid),
            (Reply::Table, Reply::Repo),
            (Reply::Table, Reply::SectionId),
            (Reply::Table, Reply::Post),
            (Reply::Table, Reply::Comment),
            (Reply::Table, Reply::To),
            (Reply::Table, Reply::Text),
            (Reply::Table, Reply::IsDisabled),
            (Reply::Table, Reply::ReasonsForDisabled),
            (Reply::Table, Reply::Edited),
            (Reply::Table, Reply::Updated),
            (Reply::Table, Reply::Created),
        ])
        .from(Reply::Table)
        .and_where(Expr::col((Reply::Table, Reply::IsDisabled)).eq(query.is_disabled))
        .and_where_option(
            query
                .section_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok())
                .map(|section| Expr::col((Reply::Table, Reply::SectionId)).eq(section)),
        )
        .and_where_option(
            query
                .q
                .as_ref()
                .map(|q| Expr::col((Reply::Table, Reply::Text)).like(format!("%{q}%"))),
        )
        .order_by(Reply::Created, Order::Asc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<ReplySampleRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        views.push(json!({
            "uri": row.uri,
            "cid": row.cid,
            "repo": row.repo,
            "section_id": row.section_id,
            "post": row.post,
            "comment": row.comment,
            "to": row.to,
            "text": row.text,
            "is_disabled": row.is_disabled,
            "reasons_for_disabled": row.reasons_for_disabled,
            "edited": row.edited,
            "updated": row.updated,
            "created": row.created,
        }));
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Reply::Table, Reply::Uri)).count_distinct())
        .from(Reply::Table)
        .and_where(Expr::col((Reply::Table, Reply::IsDisabled)).eq(query.is_disabled))
        .and_where_option(
            query
                .section_id
                .as_ref()
                .and_then(|id| id.parse::<i32>().ok())
                .map(|section| Expr::col((Reply::Table, Reply::SectionId)).eq(section)),
        )
        .and_where_option(
            query
                .q
                .map(|q| Expr::col((Reply::Table, Reply::Text)).like(format!("%{q}%"))),
        )
        .build_sqlx(PostgresQueryBuilder);
    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(json!({
        "replies": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    }))
}

pub(crate) async fn list_reply(state: &AppView, query: ReplyQuery) -> Result<Value, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Reply::Table, Reply::Uri),
            (Reply::Table, Reply::Cid),
            (Reply::Table, Reply::Repo),
            (Reply::Table, Reply::SectionId),
            (Reply::Table, Reply::Post),
            (Reply::Table, Reply::Comment),
            (Reply::Table, Reply::To),
            (Reply::Table, Reply::Text),
            (Reply::Table, Reply::IsDisabled),
            (Reply::Table, Reply::ReasonsForDisabled),
            (Reply::Table, Reply::Edited),
            (Reply::Table, Reply::Updated),
            (Reply::Table, Reply::Created),
        ])
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"reply\".\"uri\") as like_count"))
        .expr(if let Some(viewer) =&query.viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"reply\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Reply::Table)
        .and_where(Expr::col((Reply::Table, Reply::Comment)).eq(&query.comment))
        .and_where_option(query.post.map(|p| Expr::col((Reply::Table, Reply::Post)).eq(&p)))
        .and_where_option(
            query.to.map(|t| Expr::col((Reply::Table, Reply::To)).eq(&t)),
        )
        .and_where_option(query.cursor.and_then(|cursor| cursor.parse::<i64>().ok()).map(|cursor| {
            Expr::col((Reply::Table, Reply::Created)).binary(
                BinOper::GreaterThan,
                Func::cust(ToTimestamp)
                    .args([Expr::val(cursor)]),
            )
        }))
        .order_by(Reply::Created, Order::Asc)
        .limit(query.limit)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<ReplyRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let sections = Section::all(&state.db).await?;
    let admins = Administrator::all_did(&state.db).await;
    let mut views = vec![];
    for row in rows {
        let display = if let Some(viewer) = &query.viewer {
            &row.repo == viewer
                || sections
                    .get(&row.section_id)
                    .is_some_and(|section| section.owner.as_ref() == Some(viewer))
                || admins.contains(viewer)
        } else {
            false
        };
        if !row.is_disabled || display {
            let tip_count = micro_pay::payment_completed_total(
                &state.pay_url,
                &format!("{}/{}", NSID_REPLY, row.uri),
            )
            .await
            .map(|r| r.get("total").and_then(|r| r.as_i64()).unwrap_or(0))
            .unwrap_or(0);
            views.push(ReplyView {
                uri: row.uri,
                cid: row.cid,
                author: build_author(state, &row.repo).await,
                post: row.post,
                comment: row.comment,
                to: build_author(state, &row.to).await,
                text: row.text,
                is_disabled: row.is_disabled,
                reasons_for_disabled: row.reasons_for_disabled,
                edited: row.edited,
                updated: row.updated,
                created: row.created,
                like_count: row.like_count.to_string(),
                tip_count: tip_count.to_string(),
                liked: row.liked,
            });
        }
    }

    let cursor = views.last().map(|r| r.created.timestamp());
    let result = if let Some(cursor) = cursor {
        json!({
            "cursor": cursor.to_string(),
            "replies": views
        })
    } else {
        json!({
            "replies": views
        })
    };

    Ok(result)
}

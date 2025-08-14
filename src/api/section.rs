use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder, extension::postgres::PgExpr};
use sea_query_sqlx::SqlxBinder;
use serde_json::{Value, json};
use sqlx::query_as_with;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    error::AppError,
    lexicon::section::{Section, SectionRowSample, SectionView},
};

pub(crate) async fn list(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            Section::Id,
            Section::Name,
            Section::Description,
            Section::Owner,
            Section::Administrators,
        ])
        .from(Section::Table)
        .and_where(
            if let Some(Some(repo)) = query.get("repo").map(|r| r.as_str()) {
                Expr::col((Section::Table, Section::Permission))
                    .eq(0)
                    .or(Expr::col((Section::Table, Section::Administrators)).contains(repo))
                    .or(Expr::col((Section::Table, Section::Owner)).eq(repo))
            } else {
                Expr::col((Section::Table, Section::Permission)).eq(0)
            },
        )
        .order_by(Section::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<SectionRowSample> = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(rows))
}

pub(crate) async fn detail(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let id: i32 = query
        .get("id")
        .and_then(|id| id.as_str())
        .ok_or_eyre("id not be null")?
        .parse()?;

    let (sql, values) = sea_query::Query::select()
        .columns([
            Section::Id,
            Section::Name,
            Section::Description,
            Section::Owner,
            Section::Administrators,
        ])
        .from(Section::Table)
        .and_where(Expr::col(Section::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let row: SectionRowSample = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    let owner_author = if let Some(owner) = row.owner {
        get_record(&state.pds, &owner, NSID_PROFILE, "self")
            .await
            .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
            .unwrap_or(json!({
                "did": owner
            }))
    } else {
        json!({})
    };

    let mut administrators = vec![];

    if let Some(admins) = row.administrators {
        for admin in admins {
            administrators.push(
                get_record(&state.pds, &admin, NSID_PROFILE, "self")
                    .await
                    .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
                    .unwrap_or(json!({
                        "did": admin
                    })),
            );
        }
    }

    Ok(ok(SectionView {
        id: row.id.to_string(),
        name: row.name,
        description: row.description,
        owner: owner_author,
        administrators: Value::Array(administrators),
    }))
}

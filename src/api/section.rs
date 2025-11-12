use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Deserialize;
use serde_json::json;
use sqlx::query_as_with;
use utoipa::IntoParams;
use validator::Validate;

use crate::{
    AppView,
    api::build_author,
    error::AppError,
    lexicon::section::{Section, SectionRowSample, SectionView},
};

#[derive(Debug, Default, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub struct SectionQuery {
    pub repo: Option<String>,
}

#[utoipa::path(get, path = "/api/section/list", params(SectionQuery))]
pub(crate) async fn list(
    State(state): State<AppView>,
    Query(query): Query<SectionQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = Section::build_select()
        .and_where(if let Some(repo) = query.repo {
            Expr::col((Section::Table, Section::Permission))
                .eq(0)
                .or(Expr::col((Section::Table, Section::Owner)).eq(&repo))
                .or(Expr::Custom(
                    format!("'{repo}' = ANY(coalesce(section.administrators, array[]::text[]))")
                        .into(),
                ))
        } else {
            Expr::col((Section::Table, Section::Permission)).eq(0)
        })
        .order_by(Section::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<SectionRowSample> = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let owner_author = if let Some(owner) = &row.owner {
            build_author(&state, owner).await
        } else {
            json!({})
        };

        let mut administrators = vec![];

        if let Some(admins) = &row.administrators {
            for admin in admins {
                administrators.push(build_author(&state, admin).await);
            }
        }
        views.push(SectionView::build(row, owner_author, administrators));
    }

    Ok(ok(views))
}

#[derive(Debug, Default, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub struct SectionIdQuery {
    pub id: i32,
}

#[utoipa::path(get, path = "/api/section/detail", params(SectionIdQuery))]
pub(crate) async fn detail(
    State(state): State<AppView>,
    Query(query): Query<SectionIdQuery>,
) -> Result<impl IntoResponse, AppError> {
    let id: i32 = query.id;

    let (sql, values) = Section::build_select()
        .and_where(Expr::col(Section::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    let row: SectionRowSample = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    let owner_author = if let Some(owner) = &row.owner {
        build_author(&state, owner).await
    } else {
        json!({})
    };

    let mut administrators = vec![];

    if let Some(admins) = &row.administrators {
        for admin in admins {
            administrators.push(build_author(&state, admin).await);
        }
    }

    Ok(ok(SectionView::build(row, owner_author, administrators)))
}

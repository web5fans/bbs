use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder, extension::postgres::PgExpr};
use sea_query_sqlx::SqlxBinder;
use sqlx::query_as_with;

use crate::{
    AppView,
    error::AppError,
    lexicon::section::{Section, SectionRowSample},
};

pub(crate) async fn list(
    State(state): State<AppView>,
    Query(repo): Query<Option<String>>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([Section::Id, Section::Name, Section::Administrators])
        .from(Section::Table)
        .and_where(if let Some(repo) = repo {
            Expr::col((Section::Table, Section::Permission))
                .eq(0)
                .or(Expr::col((Section::Table, Section::Administrators)).contains(repo))
        } else {
            Expr::col((Section::Table, Section::Permission)).eq(0)
        })
        .order_by(Section::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<SectionRowSample> = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(rows))
}

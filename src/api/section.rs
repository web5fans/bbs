use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{extract::State, response::IntoResponse},
    ok,
};
use sea_query::{Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use sqlx::query_as_with;

use crate::{
    AppView,
    error::AppError,
    lexicon::section::{Section, SectionRow},
};

pub(crate) async fn list(State(state): State<AppView>) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            Section::Id,
            Section::Name,
            Section::Updated,
            Section::Created,
        ])
        .from(Section::Table)
        .order_by(Section::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<SectionRow> = query_as_with::<_, SectionRow, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(rows))
}

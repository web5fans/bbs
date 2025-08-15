use chrono::{DateTime, Local};
use color_eyre::Result;
use sea_query::{ColumnDef, Expr, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden)]
pub enum Status {
    Table,
    Id,
    OnlineCount,
    VisitedCount,
    Updated,
    Created,
}

impl Status {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Self::Id)
                    .integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(Self::OnlineCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Self::VisitedCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Self::Updated)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Self::Created)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;

        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([Self::Id])
            .values_panic([0.into()])
            .on_conflict(
                OnConflict::column(Self::Id)
                    .update_columns([Self::Id])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug)]
#[allow(dead_code)]
pub struct StatusRow {
    id: u32,
    online_count: u32,
    visited_count: u32,
    updated: DateTime<Local>,
    created: DateTime<Local>,
}

use chrono::NaiveDateTime;
use color_eyre::Result;
use sea_query::{ColumnDef, ColumnType, Expr, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden)]
pub enum Section {
    Table,
    Id,
    Permission,
    Name,
    Administrators,
    Updated,
    Created,
}

impl Section {
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
                ColumnDef::new(Self::Permission)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(Self::Name).string().not_null())
            .col(ColumnDef::new(Self::Administrators).array(ColumnType::String(Default::default())))
            .col(
                ColumnDef::new(Self::Updated)
                    .date_time()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(Self::Created)
                    .date_time()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;

        let sql = sea_query::Table::alter()
            .table(Self::Table)
            .add_column_if_not_exists(
                ColumnDef::new(Self::Id)
                    .integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .add_column_if_not_exists(
                ColumnDef::new(Self::Permission)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .add_column_if_not_exists(ColumnDef::new(Self::Name).string().not_null())
            .add_column_if_not_exists(
                ColumnDef::new(Self::Administrators).array(ColumnType::String(Default::default())),
            )
            .add_column_if_not_exists(
                ColumnDef::new(Self::Updated)
                    .date_time()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .add_column_if_not_exists(
                ColumnDef::new(Self::Created)
                    .date_time()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;

        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([Self::Id, Self::Name, Self::Permission])
            .values_panic([0.into(), "公告".into(), 1.into()])
            .values_panic([1.into(), "Web5技术讨论".into(), 0.into()])
            .values_panic([2.into(), "CKB RFC".into(), 0.into()])
            .on_conflict(
                OnConflict::column(Self::Id)
                    .update_columns([Self::Name])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct SectionRow {
    id: i32,
    name: String,
    permission: i32,
    administrators: Vec<String>,
    updated: NaiveDateTime,
    created: NaiveDateTime,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct SectionRowSample {
    id: i32,
    name: String,
    administrators: Vec<String>,
}

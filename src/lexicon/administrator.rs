use chrono::{DateTime, Local};
use color_eyre::Result;
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden, Debug, Clone, Copy)]
pub enum Administrator {
    Table,
    Did,
    Permission,
    Updated,
    Created,
}

impl Administrator {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Did).string().not_null().primary_key())
            .col(ColumnDef::new(Self::Permission).integer().not_null())
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
        Ok(())
    }

    pub async fn insert(db: &Pool<Postgres>, did: &str, permission: i32) -> Result<()> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([Self::Did, Self::Permission, Self::Updated, Self::Created])
            .values([
                did.into(),
                permission.into(),
                Expr::current_timestamp(),
                Expr::current_timestamp(),
            ])?
            .returning_col(Self::Did)
            .on_conflict(
                OnConflict::column(Self::Did)
                    .update_columns([Self::Permission, Self::Updated])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);

        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }

    pub async fn delete(db: &Pool<Postgres>, did: &str) -> Result<()> {
        let (sql, values) = sea_query::Query::delete()
            .from_table(Self::Table)
            .and_where(Expr::col(Self::Did).eq(did))
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }

    pub fn build_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
            .columns([
                (Administrator::Table, Administrator::Did),
                (Administrator::Table, Administrator::Permission),
                (Administrator::Table, Administrator::Updated),
                (Administrator::Table, Administrator::Created),
            ])
            .from(Administrator::Table)
            .take()
    }

    pub async fn all(db: &Pool<Postgres>) -> Vec<AdministratorRow> {
        let (sql, values) = Self::build_select().build_sqlx(PostgresQueryBuilder);
        sqlx::query_as_with(&sql, values)
            .fetch_all(db)
            .await
            .unwrap_or_default()
    }

    pub async fn all_did(db: &Pool<Postgres>) -> Vec<String> {
        let (sql, values) = sea_query::Query::select()
            .column(Administrator::Did)
            .from(Administrator::Table)
            .build_sqlx(PostgresQueryBuilder);
        let rows: Vec<(String,)> = sqlx::query_as_with(&sql, values)
            .fetch_all(db)
            .await
            .unwrap_or_default();
        rows.into_iter().map(|r| r.0).collect()
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct AdministratorRow {
    pub did: String,
    pub permission: i32,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
pub struct AdministratorView {
    pub did: Value,
    pub permission: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

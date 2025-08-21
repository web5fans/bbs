use chrono::{DateTime, Local};
use color_eyre::Result;
use sea_query::{ColumnDef, ColumnType, Expr, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden)]
pub enum Section {
    Table,
    Id,
    Permission,
    Name,
    Description,
    Owner,
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
            .col(ColumnDef::new(Self::Description).string())
            .col(ColumnDef::new(Self::Owner).string())
            .col(ColumnDef::new(Self::Administrators).array(ColumnType::String(Default::default())))
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
            .columns([Self::Id, Self::Name, Self::Permission])
            .values_panic([0.into(), "公告".into(), 1.into()])
            .values_panic([1.into(), "Web5技术讨论".into(), 0.into()])
            .values_panic([2.into(), "CKB RFC".into(), 0.into()])
            .on_conflict(
                OnConflict::column(Self::Id)
                    .update_columns([Self::Name, Self::Permission])
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
    description: Option<String>,
    permission: i32,
    owner: Option<String>,
    administrators: Option<Vec<String>>,
    updated: DateTime<Local>,
    created: DateTime<Local>,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct SectionRowSample {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub administrators: Option<Vec<String>>,
    pub visited_count: Option<i64>,
    pub post_count: Option<i64>,
    pub reply_count: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SectionView {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub owner: Value,
    pub administrators: Value,
    pub visited_count: String,
    pub post_count: String,
    pub reply_count: String,
}

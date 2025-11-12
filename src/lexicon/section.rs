use std::collections::HashMap;

use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::eyre};
use sea_query::{ColumnDef, ColumnType, Expr, ExprTrait, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_as_with};

#[derive(Iden)]
pub enum Section {
    Table,
    Id,
    Permission,
    Name,
    Description,
    Owner,
    Administrators,
    CkbAddr,
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
                ColumnDef::new(Self::CkbAddr)
                    .string()
                    .not_null()
                    .default("".to_string()),
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

        Ok(())
    }

    pub async fn all(db: &Pool<Postgres>) -> Result<HashMap<i32, SectionRowMini>> {
        let (sql, values) = sea_query::Query::select()
            .columns([
                Section::Id,
                Section::Name,
                Section::Owner,
                Section::Administrators,
            ])
            .from(Section::Table)
            .build_sqlx(PostgresQueryBuilder);
        let list: Vec<SectionRowMini> = query_as_with(&sql, values.clone())
            .fetch_all(db)
            .await
            .map_err(|e| eyre!("exec sql failed: {e}"))?;

        // list to map
        let mut map = HashMap::new();
        for row in list {
            map.insert(row.id, row);
        }

        Ok(map)
    }

    pub async fn select_by_uri(db: &Pool<Postgres>, id: i32) -> Result<SectionRowMini> {
        let (sql, values) = sea_query::Query::select()
            .columns([
                Section::Id,
                Section::Name,
                Section::Owner,
                Section::Administrators,
            ])
            .from(Section::Table)
            .and_where(Expr::col(Section::Id).eq(id))
            .build_sqlx(PostgresQueryBuilder);
        query_as_with(&sql, values.clone())
            .fetch_one(db)
            .await
            .map_err(|e| eyre!("exec sql failed: {e}"))
    }

    pub fn build_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
        .columns([
            Section::Id,
            Section::Name,
            Section::Description,
            Section::Owner,
            Section::Administrators,
            Section::CkbAddr,
        ])
        .expr(Expr::cust("(select sum(\"post\".\"visited_count\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\") as visited_count"))
        .expr(Expr::cust("(select count(\"post\".\"uri\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\") as post_count"))
        .expr(Expr::cust("(select count(\"post\".\"uri\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\" and \"post\".\"is_announcement\") as announcement_count"))
        .expr(Expr::cust("(select count(\"post\".\"uri\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\" and \"post\".\"is_top\") as top_count"))
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"section_id\" = \"section\".\"id\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"section_id\" = \"section\".\"id\") as like_count"))
        .from(Section::Table).take()
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct SectionRow {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub permission: i32,
    pub owner: Option<String>,
    pub administrators: Option<Vec<String>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct SectionRowMini {
    pub id: i32,
    pub name: String,
    pub owner: Option<String>,
    pub administrators: Option<Vec<String>>,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct SectionRowSample {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub administrators: Option<Vec<String>>,
    pub ckb_addr: String,
    pub visited_count: Option<i64>,
    pub post_count: Option<i64>,
    pub announcement_count: Option<i64>,
    pub top_count: Option<i64>,
    pub comment_count: Option<i64>,
    pub like_count: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SectionView {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub owner: Value,
    pub administrators: Value,
    pub ckb_addr: String,
    pub visited_count: String,
    pub post_count: String,
    pub announcement_count: String,
    pub top_count: String,
    pub comment_count: String,
    pub like_count: String,
}

impl SectionView {
    pub fn build(row: SectionRowSample, owner: Value, administrators: Vec<Value>) -> Self {
        Self {
            id: row.id.to_string(),
            name: row.name,
            description: row.description,
            owner,
            administrators: Value::Array(administrators),
            ckb_addr: row.ckb_addr,
            visited_count: row.visited_count.unwrap_or_default().to_string(),
            post_count: row.post_count.unwrap_or_default().to_string(),
            announcement_count: row.announcement_count.unwrap_or_default().to_string(),
            top_count: row.top_count.unwrap_or_default().to_string(),
            comment_count: row.comment_count.unwrap_or_default().to_string(),
            like_count: row.like_count.unwrap_or_default().to_string(),
        }
    }
}

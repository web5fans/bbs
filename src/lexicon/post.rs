use chrono::NaiveDateTime;
use color_eyre::Result;
use sea_query::{ColumnDef, Expr, ForeignKey, ForeignKeyAction, Iden, PostgresQueryBuilder};
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query};

use crate::lexicon::section::Section;

#[derive(Iden)]
pub enum Post {
    Table,
    Uri,
    Cid,
    Repo,
    SectionId,
    Title,
    Text,
    VisitedCount,
    Visited,
    Updated,
    Created,
}

impl Post {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Uri).string().not_null().primary_key())
            .col(ColumnDef::new(Self::Cid).string().not_null())
            .col(ColumnDef::new(Self::Repo).string().not_null())
            .col(ColumnDef::new(Self::SectionId).integer().not_null())
            .col(ColumnDef::new(Self::Title).string().not_null())
            .col(ColumnDef::new(Self::Text).string().not_null())
            .col(
                ColumnDef::new(Self::VisitedCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(ColumnDef::new(Self::Visited).date_time())
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
            .foreign_key(
                ForeignKey::create()
                    .name("section_fk")
                    .from(Self::Table, Self::SectionId)
                    .to(Section::Table, Section::Id)
                    .on_delete(ForeignKeyAction::Cascade)
                    .on_update(ForeignKeyAction::Cascade),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;

        let sql = sea_query::Table::alter()
            .table(Self::Table)
            .add_column_if_not_exists(ColumnDef::new(Self::Uri).string().not_null().primary_key())
            .add_column_if_not_exists(ColumnDef::new(Self::Cid).string().not_null())
            .add_column_if_not_exists(ColumnDef::new(Self::Repo).string().not_null())
            .add_column_if_not_exists(ColumnDef::new(Self::SectionId).integer().not_null())
            .add_column_if_not_exists(ColumnDef::new(Self::Title).string().not_null())
            .add_column_if_not_exists(ColumnDef::new(Self::Text).string().not_null())
            .add_column_if_not_exists(
                ColumnDef::new(Self::VisitedCount)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .add_column_if_not_exists(ColumnDef::new(Self::Visited).date_time())
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
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct PostRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub title: String,
    pub text: String,
    pub visited_count: i32,
    pub visited: Option<NaiveDateTime>,
    pub updated: NaiveDateTime,
    pub created: NaiveDateTime,
    #[sqlx(rename = "name")]
    pub section: String,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub actior: Value,
    pub title: String,
    pub text: String,
    pub visited_count: i32,
    pub visited: Option<NaiveDateTime>,
    pub updated: NaiveDateTime,
    pub created: NaiveDateTime,
    pub section: String,
}

use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::OptionExt};
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

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
            .col(
                ColumnDef::new(Self::Visited)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
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

        let sql = sea_query::Table::alter()
            .table(Self::Table)
            .modify_column(
                ColumnDef::new(Self::Visited)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;
        Ok(())
    }

    pub async fn insert(
        db: &Pool<Postgres>,
        repo: &str,
        post: &Value,
        uri: &str,
        cid: &str,
    ) -> Result<()> {
        let section_id = post["section_id"]
            .as_str()
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_eyre("error in section_id")?;
        let title = post["title"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in title")?;
        let text = post["text"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in text")?;
        let created = post["created"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .ok_or_eyre("error in created")?;
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([
                Self::Uri,
                Self::Cid,
                Self::Repo,
                Self::SectionId,
                Self::Title,
                Self::Text,
                Self::Created,
            ])
            .values([
                uri.into(),
                cid.into(),
                repo.into(),
                section_id.into(),
                title.into(),
                text.into(),
                created.into(),
            ])?
            .returning_col(Self::Uri)
            .build_sqlx(PostgresQueryBuilder);
        debug!("insert exec sql: {sql}");

        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct PostRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub title: String,
    pub text: String,
    pub visited_count: i32,
    pub visited: DateTime<Local>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    #[sqlx(rename = "id")]
    pub section_id: i32,
    #[sqlx(rename = "name")]
    pub section: String,
    pub reply_count: i64,
}

#[derive(Debug, Serialize)]
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub title: String,
    pub text: String,
    pub visited_count: String,
    pub visited: DateTime<Local>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub section_id: String,
    pub section: String,
    pub reply_count: String,
}

#[test]
fn test() {
    let t_str = "2025-08-15T09:13:04+08:00";
    let t = chrono::DateTime::parse_from_rfc3339(t_str)
        .map_err(|e| println!("{e}"))
        .unwrap();

    println!("t: {t:?}");

    println!("a: {}", t.to_rfc3339());
}

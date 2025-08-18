use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::OptionExt};
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden)]
pub enum Reply {
    Table,
    Uri,
    Cid,
    Repo,
    SectionId,
    Root,
    Parent,
    Text,
    Updated,
    Created,
}

impl Reply {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Uri).string().not_null().primary_key())
            .col(ColumnDef::new(Self::Cid).string().not_null())
            .col(ColumnDef::new(Self::Repo).string().not_null())
            .col(ColumnDef::new(Self::SectionId).integer().not_null())
            .col(ColumnDef::new(Self::Root).string().not_null())
            .col(ColumnDef::new(Self::Parent).string().not_null())
            .col(ColumnDef::new(Self::Text).string().not_null())
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

    pub async fn insert(
        db: &Pool<Postgres>,
        repo: &str,
        reply: &Value,
        uri: &str,
        cid: &str,
    ) -> Result<()> {
        let section_id = reply["section_id"]
            .as_str()
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_eyre("error in section_id")?;
        let root = reply["root"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in root")?;
        let parent = reply["parent"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in parent")?;
        let text = reply["text"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in text")?;
        let created = reply["created"]
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
                Self::Root,
                Self::Parent,
                Self::Text,
                Self::Created,
            ])
            .values([
                uri.into(),
                cid.into(),
                repo.into(),
                section_id.into(),
                root.into(),
                parent.into(),
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
#[allow(dead_code)]
pub struct ReplyRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub root: String,
    pub parent: String,
    pub text: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct ReplyView {
    pub uri: String,
    pub cid: String,
    pub actior: Value,
    pub root: String,
    pub parent: String,
    pub text: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

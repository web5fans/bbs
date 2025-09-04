use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::OptionExt};
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

use crate::lexicon::post::Post;

#[derive(Iden)]
pub enum Comment {
    Table,
    Uri,
    Cid,
    Repo,
    SectionId,
    To,
    Text,
    Updated,
    Created,
}

impl Comment {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Uri).string().not_null().primary_key())
            .col(ColumnDef::new(Self::Cid).string().not_null())
            .col(ColumnDef::new(Self::Repo).string().not_null())
            .col(ColumnDef::new(Self::SectionId).integer().not_null())
            .col(ColumnDef::new(Self::To).string().not_null())
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
        comment: &Value,
        uri: &str,
        cid: &str,
    ) -> Result<()> {
        let section_id = comment["section_id"]
            .as_str()
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_eyre("error in section_id")?;
        let to = comment["to"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in to")?;
        let text = comment["text"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in text")?;
        let created = comment["created"]
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
                Self::To,
                Self::Text,
                Self::Created,
            ])
            .values([
                uri.into(),
                cid.into(),
                repo.into(),
                section_id.into(),
                to.into(),
                text.into(),
                created.into(),
            ])?
            .returning_col(Self::Uri)
            .build_sqlx(PostgresQueryBuilder);
        debug!("insert exec sql: {sql}");
        db.execute(query_with(&sql, values)).await?;

        // update Post::Updated
        let (sql, values) = sea_query::Query::update()
            .table(Post::Table)
            .values([(Post::Updated, (chrono::Local::now()).into())])
            .and_where(Expr::col(Post::Uri).eq(to))
            .build_sqlx(PostgresQueryBuilder);
        debug!("update Post::Updated: {sql}");
        db.execute(query_with(&sql, values)).await.ok();
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct CommentRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub to: String,
    pub text: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub like_count: i64,
}

#[derive(Debug, Serialize)]
pub struct CommentView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub to: String,
    pub text: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub like_count: String,
}

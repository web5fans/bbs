use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::OptionExt};
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

use crate::lexicon::post::Post;

#[derive(Iden)]
pub enum Reply {
    Table,
    Uri,
    Cid,
    Repo,
    SectionId,
    Post,
    Comment,
    To,
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
            .col(ColumnDef::new(Self::Post).string().not_null())
            .col(ColumnDef::new(Self::Comment).string().not_null())
            .col(
                ColumnDef::new(Self::To)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
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
        let post = reply["post"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in post")?;
        let comment = reply["comment"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in comment")?;
        let to = reply["to"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .unwrap_or_default();
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
                Self::Post,
                Self::Comment,
                Self::To,
                Self::Text,
                Self::Updated,
                Self::Created,
            ])
            .values([
                uri.into(),
                cid.into(),
                repo.into(),
                section_id.into(),
                post.into(),
                comment.into(),
                to.into(),
                text.into(),
                Expr::current_timestamp(),
                created.into(),
            ])?
            .returning_col(Self::Uri)
            .on_conflict(
                OnConflict::column(Self::Uri)
                    .update_columns([
                        Self::Cid,
                        Self::Repo,
                        Self::SectionId,
                        Self::Post,
                        Self::Comment,
                        Self::To,
                        Self::Text,
                        Self::Updated,
                    ])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);
        debug!("insert exec sql: {sql}");
        db.execute(query_with(&sql, values)).await?;

        // update Post::Updated
        let (sql, values) = sea_query::Query::update()
            .table(Post::Table)
            .values([(Post::Updated, (chrono::Local::now()).into())])
            .and_where(Expr::col(Post::Uri).eq(comment))
            .build_sqlx(PostgresQueryBuilder);
        debug!("update Post::Updated: {sql}");
        db.execute(query_with(&sql, values)).await.ok();
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct ReplyRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub post: String,
    pub comment: String,
    pub to: String,
    pub text: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub like_count: i64,
    pub liked: bool,
}

#[derive(Debug, Serialize)]
pub struct ReplyView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub post: String,
    pub comment: String,
    pub to: Value,
    pub text: String,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub like_count: String,
    pub liked: bool,
}

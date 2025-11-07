use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::OptionExt};
use rust_decimal::Decimal;
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

use crate::lexicon::{comment::CommentRow, section::Section};

#[derive(Iden)]
pub enum Post {
    Table,
    Uri,
    Cid,
    Repo,
    SectionId,
    Title,
    Text,
    IsTop,
    IsAnnouncement,
    IsDisabled,
    ReasonsForDisabled,
    VisitedCount,
    Visited,
    Edited,
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
                ColumnDef::new(Self::IsTop)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(Self::IsAnnouncement)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(
                ColumnDef::new(Self::IsDisabled)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(Self::ReasonsForDisabled).string())
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
            .col(ColumnDef::new(Self::Edited).timestamp_with_time_zone())
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
        let edited = post["edited"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
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
                Self::Edited,
                Self::Updated,
                Self::Created,
            ])
            .values([
                uri.into(),
                cid.into(),
                repo.into(),
                section_id.into(),
                title.into(),
                text.into(),
                edited.into(),
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
                        Self::Title,
                        Self::Text,
                        Self::Edited,
                        Self::Updated,
                    ])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);
        debug!("insert exec sql: {sql}");

        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }

    pub async fn update_tag(
        db: &Pool<Postgres>,
        uri: &str,
        is_top: Option<bool>,
        is_announcement: Option<bool>,
        is_disabled: Option<bool>,
        reasons_for_disabled: Option<String>,
    ) -> Result<()> {
        let mut values = Vec::new();
        if let Some(is_top) = is_top {
            values.push((Post::IsTop, is_top.into()));
        }
        if let Some(is_announcement) = is_announcement {
            values.push((Post::IsAnnouncement, is_announcement.into()));
        }
        if let Some(is_disabled) = is_disabled {
            values.push((Post::IsDisabled, is_disabled.into()));
        }
        if let Some(reasons_for_disabled) = reasons_for_disabled {
            values.push((Post::ReasonsForDisabled, reasons_for_disabled.into()));
        }
        if values.is_empty() {
            return Ok(());
        }

        let (sql, values) = sea_query::Query::update()
            .table(Self::Table)
            .values(values)
            .and_where(Expr::col(Self::Uri).eq(uri))
            .build_sqlx(PostgresQueryBuilder);
        debug!("update_tag exec sql: {sql}");
        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }

    pub fn build_select(viewer: Option<String>) -> sea_query::SelectStatement {
        sea_query::Query::select()
        .columns([
            (Post::Table, Post::Uri),
            (Post::Table, Post::Cid),
            (Post::Table, Post::Repo),
            (Post::Table, Post::Title),
            (Post::Table, Post::Text),
            (Post::Table, Post::IsTop),
            (Post::Table, Post::IsAnnouncement),
            (Post::Table, Post::IsDisabled),
            (Post::Table, Post::ReasonsForDisabled),
            (Post::Table, Post::VisitedCount),
            (Post::Table, Post::Visited),
            (Post::Table, Post::Edited),
            (Post::Table, Post::Updated),
            (Post::Table, Post::Created),
        ])
        .columns([
            (Section::Table, Section::Id),
            (Section::Table, Section::Name),
        ])
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"post\" = \"post\".\"uri\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"post\".\"uri\") as like_count"))
        .expr(Expr::cust("(select sum(\"tip\".\"amount\") from \"tip\" where \"tip\".\"for_uri\" = \"post\".\"uri\" and \"tip\".\"state\" = 1) as tip_count"))
        .expr(if let Some(viewer) = viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"post\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        ).take()
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct PostRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub title: String,
    pub text: String,
    pub is_top: bool,
    pub is_announcement: bool,
    pub is_disabled: bool,
    pub reasons_for_disabled: Option<String>,
    pub visited_count: i32,
    pub visited: DateTime<Local>,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    #[sqlx(rename = "id")]
    pub section_id: i32,
    #[sqlx(rename = "name")]
    pub section: String,
    pub comment_count: i64,
    pub like_count: i64,
    pub tip_count: Option<Decimal>,
    pub liked: bool,
}

#[derive(Debug, Serialize)]
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub title: String,
    pub text: String,
    pub is_top: bool,
    pub is_announcement: bool,
    pub is_disabled: bool,
    pub reasons_for_disabled: Option<String>,
    pub visited_count: String,
    pub visited: DateTime<Local>,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub section_id: String,
    pub section: String,
    pub comment_count: String,
    pub like_count: String,
    pub tip_count: String,
    pub liked: bool,
}

impl PostView {
    pub fn build(row: PostRow, author: Value) -> Self {
        Self {
            uri: row.uri,
            cid: row.cid,
            author,
            title: row.title,
            text: row.text,
            is_top: row.is_top,
            is_announcement: row.is_announcement,
            is_disabled: row.is_disabled,
            reasons_for_disabled: row.reasons_for_disabled,
            visited_count: row.visited_count.to_string(),
            visited: row.visited,
            edited: row.edited,
            updated: row.updated,
            created: row.created,
            section_id: row.section_id.to_string(),
            section: row.section,
            comment_count: row.comment_count.to_string(),
            like_count: row.like_count.to_string(),
            tip_count: row.tip_count.unwrap_or(Decimal::new(0, 0)).to_string(),
            liked: row.liked,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PostRepliedView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub title: String,
    pub text: String,
    pub is_top: bool,
    pub is_announcement: bool,
    pub is_disabled: bool,
    pub reasons_for_disabled: Option<String>,
    pub comment_text: String,
    pub comment_created: DateTime<Local>,
    pub comment_disabled: bool,
    pub comment_reasons_for_disabled: Option<String>,
    pub visited_count: String,
    pub visited: DateTime<Local>,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub section_id: String,
    pub section: String,
    pub comment_count: String,
    pub like_count: String,
    pub liked: bool,
}

impl PostRepliedView {
    pub fn build(row: PostRow, author: Value, comment: CommentRow) -> Self {
        Self {
            comment_text: comment.text,
            comment_created: comment.created,
            comment_disabled: comment.is_disabled,
            comment_reasons_for_disabled: comment.reasons_for_disabled,
            uri: row.uri,
            cid: row.cid,
            author,
            title: row.title,
            text: row.text,
            is_top: row.is_top,
            is_announcement: row.is_announcement,
            is_disabled: row.is_disabled,
            reasons_for_disabled: row.reasons_for_disabled,
            visited_count: row.visited_count.to_string(),
            visited: row.visited,
            edited: row.edited,
            updated: row.updated,
            created: row.created,
            section_id: row.section_id.to_string(),
            section: row.section,
            comment_count: row.comment_count.to_string(),
            like_count: row.like_count.to_string(),
            liked: row.liked,
        }
    }
}

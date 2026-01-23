use chrono::{DateTime, Local};
use color_eyre::{
    Result,
    eyre::{OptionExt, eyre},
};
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

use crate::lexicon::{
    administrator::Administrator,
    comment::CommentRow,
    section::{Section, SectionRow},
    whitelist::Whitelist,
};

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
    IsDraft,
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
            .col(
                ColumnDef::new(Self::IsDraft)
                    .boolean()
                    .not_null()
                    .default(true),
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

        let sql = sea_query::Table::alter()
            .table(Self::Table)
            .add_column_if_not_exists(
                ColumnDef::new(Self::IsDraft)
                    .boolean()
                    .not_null()
                    .default(false),
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
        let is_draft = post["is_draft"].as_bool().unwrap_or(false);
        let is_announcement = post["is_announcement"].as_bool().unwrap_or(false);
        let is_top = post["is_top"].as_bool().unwrap_or(false);

        // check permission
        {
            if !Whitelist::select_by_did(db, repo).await {
                return Err(eyre!("Operation is not allowed!"));
            }
            let section: SectionRow = Section::select_by_id(db, section_id)
                .await
                .map_err(|e| eyre!("error in section_id: {e}"))?;
            let admins = Administrator::all_did(db).await;
            if (section.permission > 0 || is_announcement || is_top)
                && section.owner != Some(repo.to_string())
                && !admins.contains(&repo.to_string())
            {
                return Err(eyre!("Operation is not allowed!"));
            }
        }

        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([
                Self::Uri,
                Self::Cid,
                Self::Repo,
                Self::SectionId,
                Self::Title,
                Self::Text,
                Self::IsDraft,
                Self::IsAnnouncement,
                Self::IsTop,
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
                is_draft.into(),
                is_announcement.into(),
                is_top.into(),
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
                        Self::IsDraft,
                        Self::IsAnnouncement,
                        Self::IsTop,
                        Self::Edited,
                        Self::Updated,
                    ])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);

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
            (Post::Table, Post::IsDraft),
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
        .expr(Expr::cust("(select count(\"comment\".\"uri\") from \"comment\" where \"comment\".\"is_disabled\" is false and \"comment\".\"post\" = \"post\".\"uri\") as comment_count"))
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"post\".\"uri\") as like_count"))
        .expr(if let Some(viewer) = viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"post\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Post::Table)
        .left_join(
            Section::Table,
            Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
        )
        .and_where(Expr::col((Post::Table, Post::IsDraft)).eq(false)).take()
    }

    pub fn build_draft_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
            .columns([
                (Post::Table, Post::Uri),
                (Post::Table, Post::Cid),
                (Post::Table, Post::Repo),
                (Post::Table, Post::Title),
                (Post::Table, Post::Text),
                (Post::Table, Post::IsDraft),
                (Post::Table, Post::Edited),
                (Post::Table, Post::Updated),
                (Post::Table, Post::Created),
            ])
            .columns([
                (Section::Table, Section::Id),
                (Section::Table, Section::Name),
            ])
            .from(Post::Table)
            .left_join(
                Section::Table,
                Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
            )
            .and_where(Expr::col((Post::Table, Post::IsDraft)).eq(true))
            .take()
    }

    pub async fn delete(db: &Pool<Postgres>, uri: &str) -> Result<()> {
        let (sql, values) = sea_query::Query::delete()
            .from_table(Self::Table)
            .and_where(Expr::col(Self::Uri).eq(uri))
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize, Clone)]
pub struct PostRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub title: String,
    pub text: String,
    pub is_top: bool,
    pub is_announcement: bool,
    pub is_disabled: bool,
    pub is_draft: bool,
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
    pub liked: bool,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct PostDraftRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub title: String,
    pub text: String,
    pub is_draft: bool,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    #[sqlx(rename = "id")]
    pub section_id: i32,
    #[sqlx(rename = "name")]
    pub section: String,
}

#[derive(Debug, Serialize)]
pub struct PostDraftView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub title: String,
    pub text: String,
    pub is_draft: bool,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub section_id: String,
    pub section: String,
}

impl PostDraftView {
    pub fn build(row: PostDraftRow, author: Value) -> Self {
        Self {
            uri: row.uri,
            cid: row.cid,
            author,
            title: row.title,
            text: row.text,
            is_draft: row.is_draft,
            edited: row.edited,
            updated: row.updated,
            created: row.created,
            section_id: row.section_id.to_string(),
            section: row.section,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub title: String,
    pub text: String,
    pub is_top: bool,
    pub is_announcement: bool,
    pub is_disabled: bool,
    pub is_draft: bool,
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
    pub fn build(row: PostRow, author: Value, tip_count: String) -> Self {
        Self {
            uri: row.uri,
            cid: row.cid,
            author,
            title: row.title,
            text: row.text,
            is_top: row.is_top,
            is_announcement: row.is_announcement,
            is_disabled: row.is_disabled,
            is_draft: row.is_draft,
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
            tip_count,
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
    pub is_draft: bool,
    pub reasons_for_disabled: Option<String>,
    pub comment_uri: String,
    pub comment_text: String,
    pub comment_updated: DateTime<Local>,
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
    pub tip_count: String,
    pub liked: bool,
}

impl PostRepliedView {
    pub fn build(row: PostRow, author: Value, comment: CommentRow, tip_count: String) -> Self {
        Self {
            comment_uri: comment.uri,
            comment_text: comment.text,
            comment_updated: comment.updated,
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
            is_draft: row.is_draft,
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
            tip_count,
            liked: row.liked,
        }
    }
}

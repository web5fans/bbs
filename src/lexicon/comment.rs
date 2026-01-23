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
    notify::{Notify, NotifyRow, NotifyType},
    post::Post,
    resolve_uri,
    whitelist::Whitelist,
};

#[derive(Iden)]
pub enum Comment {
    Table,
    Uri,
    Cid,
    Repo,
    SectionId,
    Post,
    Text,
    IsDisabled,
    ReasonsForDisabled,
    Edited,
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
            .col(ColumnDef::new(Self::Post).string().not_null())
            .col(ColumnDef::new(Self::Text).string().not_null())
            .col(
                ColumnDef::new(Self::IsDisabled)
                    .boolean()
                    .not_null()
                    .default(false),
            )
            .col(ColumnDef::new(Self::ReasonsForDisabled).string())
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
            .add_column_if_not_exists(ColumnDef::new(Self::Edited).timestamp_with_time_zone())
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
        // check permission
        {
            if !Whitelist::select_by_did(db, repo).await {
                return Err(eyre!("Operation is not allowed!"));
            }
        }
        let section_id = comment["section_id"]
            .as_str()
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_eyre("error in section_id")?;
        let post = comment["post"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in post")?;
        let text = comment["text"]
            .as_str()
            .map(|s| s.trim_matches('\"'))
            .ok_or_eyre("error in text")?;
        let edited = comment["edited"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
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
                Self::Post,
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
                post.into(),
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
                        Self::Post,
                        Self::Text,
                        Self::Edited,
                    ])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;

        // update Post::Updated
        let (sql, values) = sea_query::Query::update()
            .table(Post::Table)
            .values([(Post::Updated, (chrono::Local::now()).into())])
            .and_where(Expr::col(Post::Uri).eq(post))
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await.ok();

        // notify
        let (receiver, _nsid, _rkey) = resolve_uri(post)?;
        Notify::insert(
            db,
            &NotifyRow {
                id: 0,
                title: "New Comment".to_string(),
                sender: repo.to_string(),
                receiver: receiver.to_string(),
                n_type: NotifyType::NewComment as i32,
                target_uri: uri.to_string(),
                amount: 0,
                readed: None,
                created: chrono::Local::now(),
            },
        )
        .await
        .ok();
        Ok(())
    }

    pub fn build_select(viewer: Option<String>) -> sea_query::SelectStatement {
        sea_query::Query::select()
        .columns([
            (Self::Table, Self::Uri),
            (Self::Table, Self::Cid),
            (Self::Table, Self::Repo),
            (Self::Table, Self::SectionId),
            (Self::Table, Self::Post),
            (Self::Table, Self::Text),
            (Self::Table, Self::IsDisabled),
            (Self::Table, Self::ReasonsForDisabled),
            (Self::Table, Self::Edited),
            (Self::Table, Self::Updated),
            (Self::Table, Self::Created),
        ])
        .expr(Expr::cust("(select count(\"like\".\"uri\") from \"like\" where \"like\".\"to\" = \"comment\".\"uri\") as like_count"))
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"comment\" = \"comment\".\"uri\") as reply_count"))
        .expr(if let Some(viewer) = &viewer {
            Expr::cust(format!("((select count(\"like\".\"uri\") from \"like\" where \"like\".\"repo\" = '{viewer}' and \"like\".\"to\" = \"comment\".\"uri\" ) > 0) as liked"))
        } else {
            Expr::cust("false as liked".to_string())
        })
        .from(Self::Table).take()
    }

    pub async fn update_tag(
        db: &Pool<Postgres>,
        uri: &str,
        is_disabled: Option<bool>,
        reasons_for_disabled: Option<String>,
    ) -> Result<()> {
        let mut values = Vec::new();
        if let Some(is_disabled) = is_disabled {
            values.push((Self::IsDisabled, is_disabled.into()));
        }
        if let Some(reasons_for_disabled) = reasons_for_disabled {
            values.push((Self::ReasonsForDisabled, reasons_for_disabled.into()));
        }
        if values.is_empty() {
            return Ok(());
        }

        values.push((Self::Updated, Expr::current_timestamp()));

        let (sql, values) = sea_query::Query::update()
            .table(Self::Table)
            .values(values)
            .and_where(Expr::col(Self::Uri).eq(uri))
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize, Clone)]
pub struct CommentRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub section_id: i32,
    pub post: String,
    pub text: String,
    pub is_disabled: bool,
    pub reasons_for_disabled: Option<String>,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub like_count: i64,
    pub liked: bool,
    pub reply_count: i64,
}

#[derive(Debug, Serialize)]
pub struct CommentView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub post: String,
    pub text: String,
    pub is_disabled: bool,
    pub reasons_for_disabled: Option<String>,
    pub edited: Option<DateTime<Local>>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
    pub like_count: String,
    pub tip_count: String,
    pub replies: Value,
    pub liked: bool,
    pub reply_count: String,
}

impl CommentView {
    pub fn build(row: CommentRow, author: Value, replies: Value, tip_count: String) -> Self {
        Self {
            uri: row.uri,
            cid: row.cid,
            author,
            post: row.post,
            text: row.text,
            is_disabled: row.is_disabled,
            reasons_for_disabled: row.reasons_for_disabled,
            edited: row.edited,
            updated: row.updated,
            created: row.created,
            like_count: row.like_count.to_string(),
            tip_count,
            replies,
            liked: row.liked,
            reply_count: row.reply_count.to_string(),
        }
    }
}

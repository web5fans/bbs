use chrono::NaiveDateTime;
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
            // .foreign_key(
            //     ForeignKey::create()
            //         .name("section_fk")
            //         .from(Self::Table, Self::SectionId)
            //         .to(Section::Table, Section::Id)
            //         .on_delete(ForeignKeyAction::Cascade)
            //         .on_update(ForeignKeyAction::Cascade),
            // )
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
            .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok())
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
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub author: Value,
    pub title: String,
    pub text: String,
    pub visited_count: String,
    pub visited: Option<NaiveDateTime>,
    pub updated: NaiveDateTime,
    pub created: NaiveDateTime,
    pub section: String,
}

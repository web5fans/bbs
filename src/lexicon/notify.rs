use chrono::{DateTime, Local};
use color_eyre::Result;
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden)]
pub enum Notify {
    Table,
    Id,
    Title,
    Sender,
    Receiver,
    TargetNSID,
    TargetDID,
    Readed,
    Created,
}

impl Notify {
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
            .col(ColumnDef::new(Self::Title).string().not_null())
            .col(ColumnDef::new(Self::Sender).string().not_null())
            .col(ColumnDef::new(Self::Receiver).string().not_null())
            .col(ColumnDef::new(Self::TargetNSID).string().not_null())
            .col(ColumnDef::new(Self::TargetDID).string().not_null())
            .col(ColumnDef::new(Self::Readed).timestamp_with_time_zone())
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

    pub fn build_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
            .columns([
                Notify::Id,
                Notify::Title,
                Notify::Sender,
                Notify::Receiver,
                Notify::TargetNSID,
                Notify::TargetDID,
                Notify::Readed,
                Notify::Created,
            ])
            .from(Notify::Table)
            .take()
    }

    pub async fn insert(db: &Pool<Postgres>, notify: &NotifyRow) -> Result<()> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Notify::Table)
            .columns([
                Notify::Title,
                Notify::Sender,
                Notify::Receiver,
                Notify::TargetNSID,
                Notify::TargetDID,
                Notify::Readed,
                Notify::Created,
            ])
            .values([
                notify.title.clone().into(),
                notify.sender.clone().into(),
                notify.receiver.clone().into(),
                notify.target_nsid.clone().into(),
                notify.target_did.clone().into(),
                notify.readed.into(),
                Expr::current_timestamp(),
            ])?
            .returning_col(Self::Id)
            .build_sqlx(PostgresQueryBuilder);

        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct NotifyRow {
    pub id: i32,
    pub title: String,
    pub sender: String,
    pub receiver: String,
    pub target_nsid: String,
    pub target_did: String,
    pub readed: Option<DateTime<Local>>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
pub struct NotifyView {
    pub id: String,
    pub title: String,
    pub sender: Value,
    pub receiver: Value,
    pub target_nsid: String,
    pub target_did: String,
    pub target: Value,
    pub readed: Option<DateTime<Local>>,
    pub created: DateTime<Local>,
}

use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::eyre};
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, Row, query};

#[derive(Debug, Clone, Copy)]
pub enum TipState {
    Prepared = 0,
    Committed = 1,
    Timeout = 2,
    Rejected = 3,
}

#[derive(Debug, Clone, Copy)]
pub enum TipCategory {
    Tip = 0,
    Donate = 1,
}

#[derive(Iden)]
pub enum Tip {
    Table,
    Id,
    Category,
    SenderDid,
    Sender,
    Receiver,
    ReceiverDid,
    Amount,
    Info,
    ForUri,
    State,
    TxHash,
    Updated,
    Created,
}

impl Tip {
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
            .col(
                ColumnDef::new(Self::Category)
                    .integer()
                    .not_null()
                    .default(0),
            )
            .col(
                ColumnDef::new(Self::SenderDid)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
            .col(ColumnDef::new(Self::Sender).string().not_null())
            .col(ColumnDef::new(Self::Receiver).string().not_null())
            .col(
                ColumnDef::new(Self::ReceiverDid)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
            .col(ColumnDef::new(Self::Amount).big_integer().not_null())
            .col(ColumnDef::new(Self::Info).string().not_null())
            .col(
                ColumnDef::new(Self::ForUri)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
            .col(ColumnDef::new(Self::State).integer().not_null().default(0))
            .col(ColumnDef::new(Self::TxHash).string())
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

    pub async fn insert(db: &Pool<Postgres>, row: &TipRow) -> Result<i32> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([
                Self::Category,
                Self::SenderDid,
                Self::Sender,
                Self::Receiver,
                Self::ReceiverDid,
                Self::Amount,
                Self::Info,
                Self::ForUri,
                Self::State,
                Self::TxHash,
                Self::Updated,
                Self::Created,
            ])
            .values([
                row.category.into(),
                row.sender_did.clone().into(),
                row.sender.clone().into(),
                row.receiver.clone().into(),
                row.receiver_did.clone().into(),
                row.amount.into(),
                row.info.clone().into(),
                row.for_uri.clone().into(),
                row.state.into(),
                row.tx_hash.clone().into(),
                Expr::current_timestamp(),
                Expr::current_timestamp(),
            ])?
            .returning_col(Self::Id)
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values)
            .fetch_one(db)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(|e| eyre!(e))
    }

    pub fn build_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
            .columns([
                (Self::Table, Self::Id),
                (Self::Table, Self::Category),
                (Self::Table, Self::Sender),
                (Self::Table, Self::SenderDid),
                (Self::Table, Self::Receiver),
                (Self::Table, Self::ReceiverDid),
                (Self::Table, Self::Amount),
                (Self::Table, Self::Info),
                (Self::Table, Self::ForUri),
                (Self::Table, Self::State),
                (Self::Table, Self::TxHash),
                (Self::Table, Self::Updated),
                (Self::Table, Self::Created),
            ])
            .from(Self::Table)
            .take()
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct TipRow {
    pub id: i32,
    pub category: i32,
    pub sender: String,
    pub sender_did: String,
    pub receiver: String,
    pub receiver_did: String,
    pub amount: i64,
    pub info: String,
    pub for_uri: String,
    pub state: i32,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct TipView {
    pub id: String,
    pub category: String,
    pub sender: String,
    pub sender_did: String,
    pub sender_author: Value,
    pub receiver: String,
    pub receiver_did: String,
    pub amount: String,
    pub info: String,
    pub for_uri: String,
    pub state: String,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct TipDetailView {
    pub id: String,
    pub category: String,
    pub sender: String,
    pub sender_did: String,
    pub sender_author: Value,
    pub receiver: String,
    pub receiver_did: String,
    pub receiver_author: Value,
    pub amount: String,
    pub info: String,
    pub for_uri: String,
    pub source: Value,
    pub state: String,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

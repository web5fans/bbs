use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::eyre};
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use sqlx::{Executor, Pool, Postgres, Row, query};

#[derive(Iden)]
pub enum Tip {
    Table,
    Id,
    SenderDid,
    Sender,
    Receiver,
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
                ColumnDef::new(Self::SenderDid)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
            .col(ColumnDef::new(Self::Sender).string().not_null())
            .col(ColumnDef::new(Self::Receiver).string().not_null())
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

        let sql = sea_query::Table::alter()
            .table(Self::Table)
            .add_column_if_not_exists(
                ColumnDef::new(Self::SenderDid)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
            .add_column_if_not_exists(
                ColumnDef::new(Self::ForUri)
                    .string()
                    .not_null()
                    .default("".to_string()),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await.ok();
        Ok(())
    }

    pub async fn insert(db: &Pool<Postgres>, row: &TipRow) -> Result<i32> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([
                Self::SenderDid,
                Self::Sender,
                Self::Receiver,
                Self::Amount,
                Self::Info,
                Self::ForUri,
                Self::State,
                Self::TxHash,
                Self::Updated,
                Self::Created,
            ])
            .values([
                row.sender_did.clone().into(),
                row.sender.clone().into(),
                row.receiver.clone().into(),
                row.amount.parse::<u64>()?.into(),
                row.info.clone().into(),
                row.for_uri.clone().into(),
                row.state.into(),
                row.tx_hash.clone().into(),
                Expr::current_timestamp(),
                Expr::current_timestamp(),
            ])?
            .returning_col(Self::Id)
            .build_sqlx(PostgresQueryBuilder);
        debug!("insert exec sql: {sql}");
        sqlx::query_with(&sql, values)
            .fetch_one(db)
            .await
            .and_then(|r| r.try_get(0))
            .map_err(|e| eyre!(e))
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct TipRow {
    pub id: i32,
    pub sender_did: String,
    pub sender: String,
    pub receiver: String,
    pub amount: String,
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
    pub sender_did: String,
    pub sender: String,
    pub receiver: String,
    pub amount: String,
    pub info: String,
    pub for_uri: String,
    pub state: String,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

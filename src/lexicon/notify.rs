use chrono::{DateTime, Local};
use color_eyre::Result;
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, ToSchema)]
pub enum NotifyType {
    NewComment = 0,
    NewReply = 1,
    NewLike = 2,
    NewTip = 3,
    NewDonate = 4,
    BeHidden = 5,
    BeDisplayed = 6,
}

#[derive(Iden, Debug, Clone, Copy)]
pub enum Notify {
    Table,
    Id,
    Title,
    Sender,
    Receiver,
    NType,
    TargetUri,
    Amount,
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
            .col(ColumnDef::new(Self::NType).integer().not_null())
            .col(ColumnDef::new(Self::TargetUri).string().not_null())
            .col(
                ColumnDef::new(Self::Amount)
                    .big_integer()
                    .not_null()
                    .default(0),
            )
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
                Notify::NType,
                Notify::TargetUri,
                Notify::Amount,
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
                Notify::NType,
                Notify::TargetUri,
                Notify::Amount,
                Notify::Readed,
                Notify::Created,
            ])
            .values([
                notify.title.clone().into(),
                notify.sender.clone().into(),
                notify.receiver.clone().into(),
                notify.n_type.into(),
                notify.target_uri.clone().into(),
                notify.amount.into(),
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
    pub n_type: i32,
    pub target_uri: String,
    pub amount: i64,
    pub readed: Option<DateTime<Local>>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
pub struct NotifyView {
    pub id: String,
    pub title: String,
    pub sender: Value,
    pub receiver: Value,
    pub n_type: String,
    pub target_uri: String,
    pub target: Value,
    pub amount: i64,
    pub readed: Option<DateTime<Local>>,
    pub created: DateTime<Local>,
}

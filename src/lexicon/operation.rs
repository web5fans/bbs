use chrono::{DateTime, Local};
use color_eyre::Result;
use sea_query::{ColumnDef, Expr, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden, Debug, Clone, Copy)]
pub enum Operation {
    Table,
    Id,
    SectionId,
    Operator,
    Action,
    Message,
    Target,
    Created,
}

impl Operation {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Self::Id)
                    .integer()
                    .auto_increment()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Self::SectionId).integer().not_null())
            .col(ColumnDef::new(Self::Operator).string().not_null())
            .col(ColumnDef::new(Self::Action).string().not_null())
            .col(ColumnDef::new(Self::Message).string())
            .col(ColumnDef::new(Self::Target).string().not_null())
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

    pub async fn insert(db: &Pool<Postgres>, row: OperationRow) -> Result<()> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([
                Self::SectionId,
                Self::Operator,
                Self::Action,
                Self::Message,
                Self::Target,
                Self::Created,
            ])
            .values([
                row.section_id.into(),
                row.operator.into(),
                row.action.into(),
                row.message.into(),
                row.target.into(),
                Expr::current_timestamp(),
            ])?
            .returning_col(Self::Id)
            .build_sqlx(PostgresQueryBuilder);

        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }

    pub fn build_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
            .columns([
                (Operation::Table, Operation::Id),
                (Operation::Table, Operation::SectionId),
                (Operation::Table, Operation::Operator),
                (Operation::Table, Operation::Action),
                (Operation::Table, Operation::Message),
                (Operation::Table, Operation::Target),
                (Operation::Table, Operation::Created),
            ])
            .from(Operation::Table)
            .take()
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct OperationRow {
    pub id: i32,
    pub section_id: i32,
    pub operator: String,
    pub action: String,
    pub message: String,
    pub target: String,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
pub struct OperationView {
    pub id: String,
    pub section_id: String,
    pub operator: Value,
    pub action: String,
    pub message: String,
    pub target: Value,
    pub created: DateTime<Local>,
}

use color_eyre::Result;
use sea_query::{ColumnDef, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden, Debug, Clone, Copy)]
pub enum Profile {
    Table,
    Did,
    Record,
}

impl Profile {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Did).string().not_null().primary_key())
            .col(ColumnDef::new(Self::Record).json_binary().default("{}"))
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;
        Ok(())
    }

    pub async fn insert(db: &Pool<Postgres>, did: &str, profile: Value) -> Result<()> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([Self::Did, Self::Record])
            .values([did.into(), profile.into()])?
            .returning_col(Self::Did)
            .on_conflict(
                OnConflict::column(Self::Did)
                    .update_columns([Self::Record])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);

        db.execute(query_with(&sql, values)).await?;
        Ok(())
    }

    pub fn build_select() -> sea_query::SelectStatement {
        sea_query::Query::select()
            .columns([
                (Profile::Table, Profile::Did),
                (Profile::Table, Profile::Record),
            ])
            .from(Profile::Table)
            .take()
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct ProfileRow {
    pub did: String,
    pub record: Value,
}

use color_eyre::Result;
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, OnConflict, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{Executor, Pool, Postgres, query, query_with};

#[derive(Iden)]
pub enum Profile {
    Table,
    Uri,
    Cid,
    Repo,
    Record,
}

impl Profile {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Uri).string().not_null().primary_key())
            .col(ColumnDef::new(Self::Cid).string().not_null())
            .col(ColumnDef::new(Self::Repo).string().not_null())
            .col(
                ColumnDef::new(Self::Record)
                    .json_binary()
                    .not_null()
                    .default(json!({})),
            )
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;
        Ok(())
    }

    pub async fn insert(
        db: &Pool<Postgres>,
        repo: &str,
        record: &Value,
        uri: &str,
        cid: &str,
    ) -> Result<()> {
        let (sql, values) = sea_query::Query::insert()
            .into_table(Self::Table)
            .columns([Self::Uri, Self::Cid, Self::Repo, Self::Record])
            .values([uri.into(), cid.into(), repo.into(), record.clone().into()])?
            .returning_col(Self::Uri)
            .on_conflict(
                OnConflict::column(Self::Uri)
                    .update_columns([Self::Cid, Self::Repo, Self::Record])
                    .to_owned(),
            )
            .build_sqlx(PostgresQueryBuilder);
        db.execute(query_with(&sql, values)).await?;

        Ok(())
    }

    pub async fn get(db: &Pool<Postgres>, uri: &str) -> Result<ProfileRow> {
        let (sql, values) = sea_query::Query::select()
            .columns([Self::Uri, Self::Cid, Self::Repo, Self::Record])
            .from(Self::Table)
            .and_where(Expr::col(Self::Uri).eq(uri))
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_as_with(&sql, values)
            .fetch_one(db)
            .await
            .map_err(Into::into)
    }
}

#[derive(sqlx::FromRow, Debug, Serialize)]
pub struct ProfileRow {
    pub uri: String,
    pub cid: String,
    pub repo: String,
    pub record: Value,
}

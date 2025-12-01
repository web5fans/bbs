use color_eyre::{Result, eyre::eyre};
use sea_query::{ColumnDef, Expr, ExprTrait, Iden, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use sqlx::{Executor, Pool, Postgres, query, query_as_with};

#[derive(Iden)]
pub enum Whitelist {
    Table,
    Did,
}

impl Whitelist {
    pub async fn init(db: &Pool<Postgres>) -> Result<()> {
        let sql = sea_query::Table::create()
            .table(Self::Table)
            .if_not_exists()
            .col(ColumnDef::new(Self::Did).string().not_null().primary_key())
            .build(PostgresQueryBuilder);
        db.execute(query(&sql)).await?;

        Ok(())
    }

    pub async fn _all(db: &Pool<Postgres>) -> Result<Vec<String>> {
        let (sql, values) = sea_query::Query::select()
            .columns([Whitelist::Did])
            .from(Whitelist::Table)
            .build_sqlx(PostgresQueryBuilder);
        let list: Vec<(String,)> = query_as_with(&sql, values.clone())
            .fetch_all(db)
            .await
            .map_err(|e| eyre!("exec sql failed: {e}"))?;

        // list to map
        let result = list.iter().map(|did| did.0.clone()).collect();

        Ok(result)
    }

    pub async fn select_by_did(db: &Pool<Postgres>, did: &str) -> bool {
        let (sql, values) = sea_query::Query::select()
            .columns([Whitelist::Did])
            .from(Whitelist::Table)
            .and_where(Expr::col(Whitelist::Did).eq(did))
            .build_sqlx(PostgresQueryBuilder);
        query_as_with::<_, (String,), _>(&sql, values.clone())
            .fetch_one(db)
            .await
            .ok()
            .is_some()
    }
}

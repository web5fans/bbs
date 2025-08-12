#[cfg(test)]
use color_eyre::Result;
#[cfg(test)]
use sqlx::{Executor, postgres::PgPoolOptions, query};

#[tokio::test]
async fn init_db() -> Result<()> {
    common_x::log::init_log_filter("info");

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:Abc1234%@localhost/postgres")
        .await?;

    // post
    db.execute(query(
        r#"
CREATE TABLE IF NOT EXISTS posts (
    uri TEXT PRIMARY KEY,
    cid TEXT NOT NULL,
    text TEXT,
    tags TEXT[],
    labels TEXT[],
    indexedAt TEXT NOT NULL
);
"#,
    ))
    .await?;

    Ok(())
}

#[tokio::test]
async fn delete_table() -> Result<()> {
    common_x::log::init_log_filter("info");

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:Abc1234%@localhost/postgres")
        .await?;

    db.execute(query("DROP TABLE IF EXISTS post;")).await?;
    db.execute(query("DROP TABLE IF EXISTS posts;")).await?;
    db.execute(query("DROP TABLE IF EXISTS tag;")).await?;
    db.execute(query("DROP TABLE IF EXISTS label;")).await?;

    Ok(())
}

#[tokio::test]
async fn list_post() -> Result<()> {
    common_x::log::init_log_filter("info");

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:Abc1234%@localhost/postgres")
        .await?;

    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        String,
        String,
        String,
        String,
        Option<Vec<String>>,
        Vec<String>,
    )> = sqlx::query_as("SELECT uri, cid, indexedAt, text, tags, labels FROM posts WHERE indexedAt > '2025-07-02T08:00:00.000Z' ORDER BY indexedAt DESC")
        .fetch_all(&db)
        .await?;
    for row in rows {
        let uri: String = row.0;
        let cid: String = row.1;
        let indexed_at: String = row.2;
        let text: String = row.3;
        let tags = row.4;
        let labels = row.5;
        info!(
            "Post - URI: {}, CID: {}, Indexed At: {}, Text: {}, Tags: {:?}, Labels: {:?}",
            uri, cid, indexed_at, text, tags, labels
        );
    }
    Ok(())
}

#[tokio::test]
async fn ban_post() -> Result<()> {
    common_x::log::init_log_filter("info");

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:Abc1234%@localhost/postgres")
        .await?;

    #[allow(clippy::type_complexity)]
    let rows: Vec<(String, Option<Vec<String>>)> =
        sqlx::query_as("SELECT uri, labels FROM posts WHERE (ARRAY['test'] && labels) IS NULL")
            .fetch_all(&db)
            .await?;
    for row in rows {
        let uri: String = row.0;
        let labels = row.1;
        info!("Post - URI: {}, Labels: {:?}", uri, labels);
    }
    Ok(())
}

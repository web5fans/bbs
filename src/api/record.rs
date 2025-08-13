use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok_simple,
};
use sea_query::PostgresQueryBuilder;
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Executor, Pool, Postgres, query_with};

use crate::{
    AppView,
    atproto::{NSID_POST, direct_writes},
    error::AppError,
    lexicon::post::Post,
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NewRecord {
    repo: String,
    rkey: String,
    value: Value,
    signing_key: String,
    root: Value,
}

pub(crate) async fn create(
    State(state): State<AppView>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    Json(new_record): Json<NewRecord>,
) -> Result<impl IntoResponse, AppError> {
    let result = direct_writes(
        &state.pds,
        auth.token(),
        &new_record.repo,
        &json!([{
            "$type": "com.atproto.web5.directWrites#create",
            "collection": NSID_POST,
            "rkey": new_record.rkey,
            "value": new_record.value
        }]),
        &new_record.signing_key,
        &new_record.root,
    )
    .await?;

    debug!("pds: {}", result);

    let uri = result
        .get("uri")
        .and_then(|uri| uri.as_str())
        .ok_or_eyre("create_record error: no uri")?;

    let cid = result
        .get("cid")
        .and_then(|cid| cid.as_str())
        .ok_or_eyre("create_record error: no cid")?;

    if let Some(Some(record_type)) = new_record.value.get("$type").map(|t| t.as_str()) {
        #[allow(clippy::single_match, clippy::collapsible_match)]
        match record_type {
            NSID_POST => {
                insert_post(&state.db, &new_record.repo, &new_record.value, uri, cid).await?;
            }
            _ => {}
        }
    }

    Ok(ok_simple())
}

async fn insert_post(
    db: &Pool<Postgres>,
    repo: &str,
    post: &Value,
    uri: &str,
    cid: &str,
) -> Result<(), AppError> {
    let (sql, values) = sea_query::Query::insert()
        .into_table(Post::Table)
        .columns([
            Post::Uri,
            Post::Cid,
            Post::Repo,
            Post::SectionId,
            Post::Title,
            Post::Text,
            Post::Created,
        ])
        .values([
            uri.into(),
            cid.into(),
            repo.into(),
            post["section_id"].clone().into(),
            post["title"].clone().into(),
            post["text"].clone().into(),
            post["created"].clone().into(),
        ])?
        .returning_col(Post::Uri)
        .build_sqlx(PostgresQueryBuilder);
    debug!("write_posts exec sql: {sql}");

    db.execute(query_with(&sql, values))
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    Ok(())
}

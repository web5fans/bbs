use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok_simple,
};
use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use sea_query::{Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use sqlx::query_as_with;
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    atproto::{NSID_COMMENT, NSID_POST, NSID_REPLY},
    error::AppError,
    indexer::did_document,
    lexicon::{comment::Comment, post::Post, reply::Reply, section::Section},
};

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct UpdateTagParams {
    pub nsid: String,
    pub uri: String,
    pub is_top: Option<bool>,
    pub is_announcement: Option<bool>,
    pub is_disabled: Option<bool>,
    pub reasons_for_disabled: Option<String>,
}

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct UpdateTagBody {
    pub params: UpdateTagParams,
    pub did: String,
    #[validate(length(equal = 57))]
    pub signing_key_did: String,
    pub signed_bytes: String,
}

#[utoipa::path(post, path = "/api/admin/update_tag")]
pub(crate) async fn update_tag(
    State(state): State<AppView>,
    Json(body): Json<UpdateTagBody>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let did_doc = did_document(&state.indexer, body.did.as_str())
        .await
        .map_err(|e| {
            debug!("call indexer failed: {e}");
            AppError::ValidateFailed("get did doc failed".to_string())
        })?;

    if body.signing_key_did
        != did_doc
            .pointer("/verificationMethods/atproto")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
    {
        return Err(AppError::ValidateFailed(
            "signing_key_did not match".to_string(),
        ));
    }
    let section_id = match body.params.nsid.as_str() {
        NSID_POST => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::SectionId)])
                .from(Post::Table)
                .and_where(Expr::col(Post::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (i32,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row.0
        }
        NSID_REPLY => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Reply::Table, Reply::SectionId)])
                .from(Reply::Table)
                .and_where(Expr::col(Reply::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (i32,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row.0
        }
        NSID_COMMENT => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Comment::Table, Comment::SectionId)])
                .from(Comment::Table)
                .and_where(Expr::col(Comment::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (i32,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row.0
        }
        _ => return Err(eyre!("nsid is not allowed!").into()),
    };

    let section_row = Section::select_by_uri(&state.db, section_id)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    if section_row.owner == Some(body.did.clone())
        || section_row
            .administrators
            .unwrap_or_default()
            .contains(&body.did)
    {
        // verify signature
        let verifying_key: VerifyingKey = body
            .signing_key_did
            .split_once("did:key:z")
            .and_then(|(_, key)| {
                let bytes = bs58::decode(key).into_vec().ok()?;
                VerifyingKey::from_sec1_bytes(&bytes[2..]).ok()
            })
            .ok_or_else(|| AppError::ValidateFailed("invalid signing_key_did".to_string()))?;
        let signature = hex::decode(&body.signed_bytes)
            .map(|bytes| Signature::from_slice(&bytes).map_err(|e| eyre!(e)))??;

        let unsigned_bytes = serde_ipld_dagcbor::to_vec(&body.params)?;
        verifying_key
            .verify(&unsigned_bytes, &signature)
            .map_err(|e| {
                debug!("verify signature failed: {e}");
                AppError::ValidateFailed("verify signature failed".to_string())
            })?;
        match body.params.nsid.as_str() {
            NSID_POST => {
                Post::update_tag(
                    &state.db,
                    &body.params.uri,
                    body.params.is_top,
                    body.params.is_announcement,
                    body.params.is_disabled,
                    body.params.reasons_for_disabled,
                )
                .await?;
            }
            NSID_REPLY => {
                Reply::update_tag(
                    &state.db,
                    &body.params.uri,
                    body.params.is_disabled,
                    body.params.reasons_for_disabled,
                )
                .await?;
            }
            NSID_COMMENT => {
                Comment::update_tag(
                    &state.db,
                    &body.params.uri,
                    body.params.is_disabled,
                    body.params.reasons_for_disabled,
                )
                .await?;
            }
            _ => return Err(eyre!("nsid is not allowed!").into()),
        }
    } else {
        return Err(AppError::ValidateFailed(
            "only section administrator can update post tag".to_string(),
        ));
    }

    Ok(ok_simple())
}

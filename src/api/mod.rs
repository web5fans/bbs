use color_eyre::{
    Result,
    eyre::{OptionExt, eyre},
};
use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use sea_query::{BinOper, Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::query_as_with;
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
};
use validator::Validate;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    ckb::get_ckb_addr_by_did,
    lexicon::{
        administrator::{Administrator, AdministratorRow},
        comment::Comment,
        like::Like,
        post::Post,
        profile::Profile,
        section::{Section, SectionRowSample},
    },
};

pub(crate) mod admin;
pub(crate) mod comment;
pub(crate) mod donate;
pub(crate) mod like;
pub(crate) mod notify;
pub(crate) mod post;
pub(crate) mod record;
pub(crate) mod reply;
pub(crate) mod repo;
pub(crate) mod section;
pub(crate) mod tip;
pub(crate) mod whitelist;

#[derive(OpenApi, Debug, Clone, Copy)]
#[openapi(
    modifiers(&SecurityAddon),
    paths(
        admin::update_tag,
        admin::update_owner,
        admin::update_section,
        admin::create_section,
        admin::add_whitelist,
        admin::delete_whitelist,
        admin::list,
        admin::add,
        admin::operations,
        admin::delete,
        record::create,
        record::update,
        record::delete,
        section::list,
        section::detail,
        post::list,
        post::page,
        post::top,
        post::detail,
        post::commented,
        post::commented_page,
        post::list_draft,
        post::detail_draft,
        comment::list,
        reply::list,
        reply::page,
        repo::profile,
        repo::login_info,
        like::list,
        tip::prepare,
        tip::transfer,
        tip::list_by_for,
        tip::expense_details,
        tip::income_details,
        tip::stats,
        donate::prepare,
        donate::transfer,
        notify::list,
        notify::read,
        notify::unread_num,
        whitelist::list,
    ),
    components(schemas(
        SignedBody<admin::UpdateTagParams>,
        SignedBody<admin::UpdateOwnerParams>,
        SignedBody<admin::UpdateSectionParams>,
        SignedBody<admin::CreateSectionParams>,
        SignedBody<admin::WhitelistParams>,
        SignedBody<admin::UpdateAdminParams>,
        record::NewRecord,
        post::PostQuery,
        post::PostPageQuery,
        post::TopQuery,
        post::DraftQuery,
        comment::CommentQuery,
        reply::ReplyQuery,
        reply::ReplyPageQuery,
        like::LikeQuery,
        SignedBody<tip::TipParams>,
        tip::TipsQuery,
        tip::DetailQuery,
        SignedBody<donate::DonateParams>,
        notify::NotifyQuery,
        notify::NotifyReadQuery,
        crate::lexicon::notify::NotifyType,
    ))
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "Authorization",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("Authorization"))),
            )
        }
    }
}

pub(crate) struct ToTimestamp;

impl sea_query::Iden for ToTimestamp {
    fn unquoted(&self) -> &str {
        "to_timestamp"
    }
}

pub(crate) async fn build_author(state: &AppView, repo: &str) -> Value {
    if !repo.starts_with("did:") {
        return Value::String(repo.to_string());
    }
    // Get post count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Post::Table, Post::Uri)).count())
        .from(Post::Table)
        .and_where(Expr::col(Post::Repo).eq(repo))
        .and_where(Expr::col(Post::IsDraft).eq(false))
        .and_where(Expr::col((Post::Table, Post::SectionId)).binary(BinOper::NotEqual, 0))
        .build_sqlx(PostgresQueryBuilder);
    let post_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get comment count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Comment::Table, Comment::Uri)).count())
        .from(Comment::Table)
        .and_where(Expr::col(Comment::Repo).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    let comment_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get like count
    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Like::Table, Like::Uri)).count())
        .from(Like::Table)
        .and_where(Expr::col(Like::To).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    let like_count_row: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .unwrap_or((0,));

    // Get profile
    let mut author = get_profile(state, repo).await.unwrap_or(json!({
        "did": repo
    }));
    if let Ok(ckb_addr) = get_ckb_addr_by_did(&state.ckb_client, &state.ckb_net, repo).await {
        author["ckb_addr"] = Value::String(ckb_addr);
    }
    author["did"] = Value::String(repo.to_owned());
    author["post_count"] = Value::String(post_count_row.0.to_string());
    author["comment_count"] = Value::String(comment_count_row.0.to_string());
    author["like_count"] = Value::String(like_count_row.0.to_string());

    let (sql, values) = Administrator::build_select()
        .and_where(Expr::col(Administrator::Did).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    let row: Option<AdministratorRow> = sqlx::query_as_with(&sql, values)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);
    if let Some(admin) = row {
        let mut tags = author["tags"]
            .as_array_mut()
            .unwrap_or(&mut vec![])
            .to_vec();
        tags.push(json!({"admin": admin.permission}));
        author["tags"] = serde_json::Value::Array(tags);
    }

    let (sql, values) = Section::build_select()
        .and_where(Expr::col(Section::Owner).eq(repo))
        .build_sqlx(PostgresQueryBuilder);
    let row: Option<SectionRowSample> = sqlx::query_as_with(&sql, values)
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);
    if let Some(section) = row {
        let mut tags = author["tags"]
            .as_array_mut()
            .unwrap_or(&mut vec![])
            .to_vec();
        tags.push(json!({"owner": section.name}));
        author["tags"] = serde_json::Value::Array(tags);
    }

    author
}

async fn get_profile(state: &AppView, repo: &str) -> Result<Value> {
    if let Ok(profile_row) = Profile::get(&state.db, repo).await {
        Ok(profile_row.record)
    } else {
        let record = get_record(&state.pds, repo, NSID_PROFILE, "self").await?;
        let value = record.get("value").ok_or_eyre("get profile failed")?;
        let uri = record
            .get("uri")
            .and_then(|uri| uri.as_str())
            .ok_or_eyre("get profile failed")?;
        let cid = record
            .get("cid")
            .and_then(|cid| cid.as_str())
            .ok_or_eyre("get profile failed")?;
        Profile::insert(&state.db, repo, value, uri, cid).await?;
        Ok(record)
    }
}

pub trait SignedParam: Default + ToSchema + Serialize + Validate {
    fn timestamp(&self) -> i64;
}

#[derive(Default, ToSchema, Serialize, Deserialize, Validate)]
pub struct SignedBody<SignedParam> {
    pub params: SignedParam,
    pub did: String,
    #[validate(length(equal = 57))]
    pub signing_key_did: String,
    pub signed_bytes: String,
}

impl<T: SignedParam> SignedBody<T> {
    pub async fn verify_signature(&self, indexer_did_url: &str) -> color_eyre::Result<()> {
        // verify timestamp
        let timestamp =
            chrono::DateTime::from_timestamp_secs(self.params.timestamp()).unwrap_or_default();
        let now = chrono::Utc::now();
        let delta = (now - timestamp).abs();
        if delta > chrono::Duration::minutes(5) {
            return Err(eyre!("timestamp is invalid"));
        }

        // verify did
        let did_doc = crate::indexer::did_document(indexer_did_url, &self.did)
            .await
            .map_err(|e| eyre!("get did doc failed: {e}"))?;

        if self.signing_key_did
            != did_doc
                .pointer("/verificationMethods/atproto")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
        {
            return Err(eyre!("signing_key_did not match"));
        }

        // verify signature
        let verifying_key: VerifyingKey = self
            .signing_key_did
            .split_once("did:key:z")
            .and_then(|(_, key)| {
                let bytes = bs58::decode(key).into_vec().ok()?;
                VerifyingKey::from_sec1_bytes(&bytes[2..]).ok()
            })
            .ok_or_eyre("invalid signing_key_did")?;
        let signature = hex::decode(self.signed_bytes.clone())
            .map(|bytes| Signature::from_slice(&bytes).map_err(|e| eyre!(e)))??;

        let unsigned_bytes = serde_ipld_dagcbor::to_vec(&self.params)?;
        verifying_key
            .verify(&unsigned_bytes, &signature)
            .map_err(|e| eyre!("verify signature failed: {e}"))
    }
}

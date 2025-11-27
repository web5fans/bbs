use color_eyre::{Result, eyre::eyre};
use common_x::restful::{
    axum::{
        Json,
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use sea_query::{Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::query_as_with;
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

use crate::{
    AppView,
    api::build_author,
    atproto::{NSID_COMMENT, NSID_COMMUNITY, NSID_POST, NSID_REPLY, NSID_SECTION},
    ckb::get_ckb_addr_by_did,
    error::AppError,
    indexer::did_document,
    lexicon::{
        comment::Comment,
        post::Post,
        reply::Reply,
        section::Section,
        tip::{TipCategory, TipRow, TipState, TipView},
    },
    micro_pay,
};

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct TipParams {
    pub nsid: String,
    pub uri: String,
    pub sender: String,
    pub amount: String,
}

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct TipBody {
    pub params: TipParams,
    pub did: String,
    #[validate(length(equal = 57))]
    pub signing_key_did: String,
    pub signed_bytes: String,
}

#[utoipa::path(post, path = "/api/tip/prepare")]
pub(crate) async fn prepare(
    State(state): State<AppView>,
    Json(body): Json<TipBody>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    validate_signed(&state.indexer, &body).await?;

    let (receiver_did, section_ckb_addr, is_announcement) = match body.params.nsid.as_str() {
        NSID_POST => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::Repo)])
                .columns([
                    (Section::Table, Section::CkbAddr),
                    (Section::Table, Section::Id),
                ])
                .from(Post::Table)
                .left_join(
                    Section::Table,
                    Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
                )
                .and_where(Expr::col(Post::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String, i32) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            if row.2 == 0 {
                (row.1.clone(), row.1, true)
            } else {
                (row.0, row.1, false)
            }
        }
        NSID_COMMENT => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Comment::Table, Comment::Repo)])
                .columns([(Section::Table, Section::CkbAddr)])
                .from(Comment::Table)
                .left_join(
                    Section::Table,
                    Expr::col((Comment::Table, Comment::SectionId))
                        .equals((Section::Table, Section::Id)),
                )
                .and_where(Expr::col(Comment::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            (row.0, row.1, false)
        }
        NSID_REPLY => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Reply::Table, Reply::Repo)])
                .columns([(Section::Table, Section::CkbAddr)])
                .from(Reply::Table)
                .left_join(
                    Section::Table,
                    Expr::col((Reply::Table, Reply::SectionId))
                        .equals((Section::Table, Section::Id)),
                )
                .and_where(Expr::col(Reply::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            (row.0, row.1, false)
        }
        _ => {
            return Err(AppError::ValidateFailed("unsupported nsid".to_string()));
        }
    };

    let receiver = if is_announcement {
        state.bbs_ckb_addr.clone()
    } else {
        get_ckb_addr_by_did(&state.ckb_client, &state.ckb_net, &receiver_did)
            .await
            .map_err(|e| {
                debug!("get ckb addr by did failed: {e}");
                AppError::ValidateFailed("get receiver ckb addr failed".to_string())
            })?
    };

    let mut tip_row = TipRow {
        id: -1,
        category: TipCategory::Tip as i32,
        sender_did: body.did.clone(),
        sender: body.params.sender.clone(),
        receiver,
        receiver_did,
        amount: body.params.amount.parse::<i64>()?,
        info: format!("{}/{}", body.params.nsid, body.params.uri),
        state: TipState::Prepared as i32,
        tx_hash: None,
        updated: chrono::Local::now(),
        created: chrono::Local::now(),
    };

    let split_receivers = if is_announcement {
        json!([])
    } else {
        json!([
            {
                "address": &state.bbs_ckb_addr,
                "receiverDid": &state.bbs_ckb_addr,
                "splitRate": 10
            },
            {
                "address": &section_ckb_addr,
                "receiverDid": &section_ckb_addr,
                "splitRate": 20
            }
        ])
    };

    let result = micro_pay::payment_prepare(
        &state.pay_url,
        &json!({
            "sender": &tip_row.sender,
            "senderDid": &tip_row.sender_did,
            "receiver": &tip_row.receiver,
            "receiverDid": &tip_row.receiver_did,
            "category": &tip_row.category,
            "amount": &tip_row.amount,
            "info": &tip_row.info,
            "splitReceivers": split_receivers
        }),
    )
    .await?;

    if let Some(err) = result.get("error") {
        return Err(AppError::MicroPayIncomplete(
            result.get("code").unwrap_or(err).to_string(),
        ));
    }

    tip_row.tx_hash = result
        .get("txHash")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let author = build_author(&state, &tip_row.sender_did).await;
    let tip = TipView {
        id: tip_row.id.to_string(),
        category: tip_row.category.to_string(),
        sender_did: tip_row.sender_did.clone(),
        sender_author: author,
        sender: tip_row.sender.clone(),
        receiver: tip_row.receiver.clone(),
        receiver_did: tip_row.receiver_did.clone(),
        amount: tip_row.amount.to_string(),
        info: tip_row.info.clone(),
        state: tip_row.state.to_string(),
        tx_hash: tip_row.tx_hash.clone(),
        updated: tip_row.updated,
        created: tip_row.created,
    };

    Ok(ok(json!({
        "tip": tip,
        "payment": result,
    })))
}

#[utoipa::path(post, path = "/api/tip/transfer")]
pub(crate) async fn transfer(
    State(state): State<AppView>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let result = micro_pay::payment_transfer(&state.pay_url, &body).await?;
    Ok(ok(result))
}

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct TipsQuery {
    pub nsid: String,
    pub uri: String,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for TipsQuery {
    fn default() -> Self {
        Self {
            nsid: String::new(),
            uri: String::new(),
            page: 1,
            per_page: 20,
        }
    }
}

#[utoipa::path(post, path = "/api/tip/list")]
pub(crate) async fn list_by_for(
    State(state): State<AppView>,
    Json(query): Json<TipsQuery>,
) -> Result<impl IntoResponse, AppError> {
    let q = format!(
        "info={}/{}&limit={}&offset={}",
        &query.nsid,
        &query.uri,
        query.per_page,
        query.per_page * (query.page - 1)
    );
    let row = micro_pay::payment_completed(&state.pay_url, &q).await?;
    let mut items: Vec<Value> = row
        .get("items")
        .and_then(|items| items.as_array())
        .unwrap_or(&vec![])
        .to_vec();
    for item in &mut items {
        if let Some(sender_did) = item.get("senderDid").and_then(|i| i.as_str()) {
            let sender_author = build_author(&state, sender_did).await;
            item["sender_author"] = sender_author;
        }
    }

    let total = row
        .pointer("/pagination/count")
        .and_then(|i| i.as_i64())
        .unwrap_or(0);

    Ok(ok(json!({
        "tips": items,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total
    })))
}

#[derive(Debug, Validate, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct DetailQuery {
    pub start: Option<String>,
    pub end: Option<String>,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
    pub category: Option<u8>,
    pub did: String,
}

impl Default for DetailQuery {
    fn default() -> Self {
        Self {
            start: None,
            end: None,
            page: 1,
            per_page: 20,
            category: None,
            did: String::new(),
        }
    }
}

#[utoipa::path(post, path = "/api/tip/expense_details")]
pub(crate) async fn expense_details(
    State(state): State<AppView>,
    Json(query): Json<DetailQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let mut q: Vec<(&str, String)> = vec![];
    if let Some(category) = &query.category {
        q.push(("category", category.to_string()));
    }
    if let Some(start) = query.start {
        q.push(("start", start));
    }
    if let Some(end) = query.end {
        q.push(("end", end));
    }
    let per_page = query.per_page.to_string();
    q.push(("limit", per_page));
    let offset = (query.per_page * (query.page - 1)).to_string();
    q.push(("offset", offset));

    let row = micro_pay::payment_sender_did(&state.pay_url, &query.did, &q).await?;
    let mut items: Vec<Value> = row
        .get("items")
        .and_then(|items| items.as_array())
        .unwrap_or(&vec![])
        .to_vec();
    for item in &mut items {
        if let Some(info) = item.get("info").and_then(|i| i.as_str())
            && let Ok(source) = get_source(&state, info).await
        {
            item["source"] = source;
        };
        if let Some(receiver_did) = item.get("receiverDid").and_then(|i| i.as_str()) {
            let receiver_author = build_author(&state, receiver_did).await;
            item["receiver_author"] = receiver_author;
        }
    }

    let total = row
        .pointer("/pagination/count")
        .and_then(|i| i.as_i64())
        .unwrap_or(0);

    Ok(ok(json!({
        "tips": items,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total
    })))
}

#[utoipa::path(post, path = "/api/tip/income_details")]
pub(crate) async fn income_details(
    State(state): State<AppView>,
    Json(query): Json<DetailQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let mut q: Vec<(&str, String)> = vec![];
    if let Some(category) = &query.category {
        q.push(("category", category.to_string()));
    }
    if let Some(start) = query.start {
        q.push(("start", start));
    }
    if let Some(end) = query.end {
        q.push(("end", end));
    }
    let per_page = query.per_page.to_string();
    q.push(("limit", per_page));
    let offset = (query.per_page * (query.page - 1)).to_string();
    q.push(("offset", offset));

    let row = micro_pay::payment_receiver_did(&state.pay_url, &query.did, &q).await?;
    let mut items: Vec<Value> = row
        .get("items")
        .and_then(|items| items.as_array())
        .unwrap_or(&vec![])
        .to_vec();
    for item in &mut items {
        if let Some(info) = item.get("info").and_then(|i| i.as_str())
            && let Ok(source) = get_source(&state, info).await
        {
            item["source"] = source;
        };
        if let Some(sender_did) = item.get("senderDid").and_then(|i| i.as_str()) {
            let sender_author = build_author(&state, sender_did).await;
            item["sender_author"] = sender_author;
        }
    }

    let total = row
        .pointer("/pagination/count")
        .and_then(|i| i.as_i64())
        .unwrap_or(0);

    Ok(ok(json!({
        "tips": items,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total
    })))
}

#[derive(Debug, Default, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub struct DidQuery {
    pub did: String,
}

#[utoipa::path(get, path = "/api/tip/stats", params(DidQuery))]
pub(crate) async fn stats(
    State(state): State<AppView>,
    Query(query): Query<DidQuery>,
) -> Result<impl IntoResponse, AppError> {
    let result = micro_pay::payment_did_stats(&state.pay_url, &query.did).await?;
    Ok(ok(result))
}

async fn validate_signed(indexer: &str, body: &TipBody) -> Result<(), AppError> {
    let did_doc = did_document(indexer, body.did.as_str())
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
        })
}

async fn get_source(state: &AppView, info: &str) -> Result<Value, AppError> {
    let (nsid, uri) = info.split_once("/").unwrap_or(("", ""));
    let source = match nsid {
        NSID_POST => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::Title)])
                .from(Post::Table)
                .and_where(Expr::col(Post::Uri).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            json!({
                "nsid": nsid,
                "uri": uri,
                "title": row.0,
            })
        }
        NSID_COMMENT => {
            let (sql, values) = sea_query::Query::select()
                .columns([
                    (Comment::Table, Comment::Text),
                    (Comment::Table, Comment::Post),
                ])
                .from(Comment::Table)
                .and_where(Expr::col(Comment::Uri).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            json!({
                "nsid": nsid,
                "uri": uri,
                "post": row.1,
                "text": row.0,
            })
        }
        NSID_REPLY => {
            let (sql, values) = sea_query::Query::select()
                .columns([
                    (Reply::Table, Reply::Text),
                    (Reply::Table, Reply::Post),
                    (Reply::Table, Reply::Comment),
                    (Reply::Table, Reply::To),
                ])
                .from(Reply::Table)
                .and_where(Expr::col(Reply::Uri).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String, String, String) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            json!({
                "nsid": nsid,
                "uri": uri,
                "text": row.0,
                "post": row.1,
                "comment": row.2,
                "to": row.3,
            })
        }
        NSID_SECTION => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Section::Table, Section::Name)])
                .from(Section::Table)
                .and_where(Expr::col(Section::CkbAddr).eq(uri))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            json!({
                "nsid": nsid,
                "id": uri,
                "name": row.0,
            })
        }
        NSID_COMMUNITY => {
            json!({
                "nsid": nsid,
                "name": "BBS 社区",
            })
        }
        _ => {
            json!({
                "nsid": nsid,
                "uri": uri,
            })
        }
    };

    Ok(source)
}

#[test]
fn test() {
    let a = chrono::Local::now();
    let now = chrono::Local::now();
    let five_minutes_ago = now - chrono::Duration::minutes(5);
    println!("now: {}", now);
    println!("five_minutes_ago: {}", five_minutes_ago);
    assert!(a >= five_minutes_ago);
}

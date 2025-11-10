use chrono::{DateTime, Local};
use color_eyre::{Result, eyre::eyre};
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok,
};
use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use sea_query::{Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::query_as_with;
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    api::build_author,
    atproto::{NSID_COMMENT, NSID_POST, NSID_REPLY},
    ckb::{get_ckb_addr_by_did, get_tx_status},
    error::AppError,
    indexer::did_document,
    lexicon::{
        comment::Comment,
        post::Post,
        reply::Reply,
        section::Section,
        tip::{Tip, TipRow, TipView},
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

    let (receiver_did, section_ckb_addr) = match body.params.nsid.as_str() {
        NSID_POST => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::Repo)])
                .columns([(Section::Table, Section::CkbAddr)])
                .from(Post::Table)
                .left_join(
                    Section::Table,
                    Expr::col((Post::Table, Post::SectionId)).equals((Section::Table, Section::Id)),
                )
                .and_where(Expr::col(Post::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (String, String) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row
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
            row
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
            row
        }
        _ => {
            return Err(AppError::ValidateFailed("unsupported nsid".to_string()));
        }
    };

    let receiver = get_ckb_addr_by_did(&state.ckb_client, &receiver_did)
        .await
        .map_err(|e| {
            debug!("get ckb addr by did failed: {e}");
            AppError::ValidateFailed("get receiver ckb addr failed".to_string())
        })?;

    let mut tip_row = TipRow {
        id: -1,
        sender_did: body.did.clone(),
        sender: body.params.sender.clone(),
        receiver,
        receiver_did,
        amount: body.params.amount.parse::<i64>()?,
        info: format!("{}/{}", body.params.nsid, body.params.uri),
        for_uri: body.params.uri.clone(),
        state: 0,
        tx_hash: None,
        updated: chrono::Local::now(),
        created: chrono::Local::now(),
    };

    debug!("section_ckb_addr: {}", section_ckb_addr);

    let result = micro_pay::payment_prepare(
        &state.pay_url,
        &json!({
            "sender": &tip_row.sender,
            "sender_did": &tip_row.sender_did,
            "receiver": &tip_row.receiver,
            "receiver_did": &tip_row.receiver_did,
            "amount": &tip_row.amount,
            "info": &tip_row.info,
            "splitReceivers": [
                {
                    "address": &state.bbs_ckb_addr,
                    "splitRate": 10
                },
                {
                    "address": &section_ckb_addr,
                    "splitRate": 20
                }
            ]
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

    tip_row.id = Tip::insert(&state.db, &tip_row).await?;

    let author = build_author(&state, &tip_row.sender_did).await;
    let tip = TipView {
        id: tip_row.id.to_string(),
        sender_did: tip_row.sender_did.clone(),
        sender_author: author,
        sender: tip_row.sender.clone(),
        receiver: tip_row.receiver.clone(),
        receiver_did: tip_row.receiver_did.clone(),
        amount: tip_row.amount.to_string(),
        info: tip_row.info.clone(),
        for_uri: tip_row.for_uri.clone(),
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
    pub for_uri: String,
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for TipsQuery {
    fn default() -> Self {
        Self {
            for_uri: String::new(),
            page: 1,
            per_page: 20,
        }
    }
}

#[utoipa::path(post, path = "/api/tip/list")]
pub(crate) async fn list_by_for(
    State(state): State<AppView>,
    Json(body): Json<TipsQuery>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Tip::Table, Tip::Id),
            (Tip::Table, Tip::SenderDid),
            (Tip::Table, Tip::Sender),
            (Tip::Table, Tip::Receiver),
            (Tip::Table, Tip::Amount),
            (Tip::Table, Tip::Info),
            (Tip::Table, Tip::ForUri),
            (Tip::Table, Tip::State),
            (Tip::Table, Tip::TxHash),
            (Tip::Table, Tip::Updated),
            (Tip::Table, Tip::Created),
        ])
        .from(Tip::Table)
        .and_where(Expr::col(Tip::ForUri).eq(&body.for_uri))
        .and_where(Expr::col(Tip::State).eq(1))
        .order_by((Tip::Table, Tip::Created), sea_query::Order::Desc)
        .offset(body.per_page * (body.page - 1))
        .limit(body.per_page)
        .build_sqlx(PostgresQueryBuilder);
    let rows: Vec<TipRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;
    let mut result: Vec<TipView> = Vec::new();
    for tip_row in &rows {
        let author = build_author(&state, &tip_row.sender_did).await;
        result.push(TipView {
            id: tip_row.id.to_string(),
            sender_did: tip_row.sender_did.clone(),
            sender_author: author,
            sender: tip_row.sender.clone(),
            receiver: tip_row.receiver.clone(),
            receiver_did: tip_row.receiver_did.clone(),
            amount: tip_row.amount.to_string(),
            info: tip_row.info.clone(),
            for_uri: tip_row.for_uri.clone(),
            state: tip_row.state.to_string(),
            tx_hash: tip_row.tx_hash.clone(),
            updated: tip_row.updated,
            created: tip_row.created,
        });
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Tip::Table, Tip::Id)).count())
        .from(Tip::Table)
        .and_where(Expr::col(Tip::ForUri).eq(&body.for_uri))
        .and_where(Expr::col(Tip::State).eq(1))
        .build_sqlx(PostgresQueryBuilder);

    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(json!({
        "tips": result,
        "page": body.page,
        "per_page": body.per_page,
        "total":  total.0
    })))
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
    debug!("unsigned_bytes: {}", hex::encode(&unsigned_bytes));
    verifying_key
        .verify(&unsigned_bytes, &signature)
        .map_err(|e| {
            debug!("verify signature failed: {e}");
            AppError::ValidateFailed("verify signature failed".to_string())
        })
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

pub async fn check_tip_tx(state: AppView) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    let (sql, values) = sea_query::Query::select()
        .columns([
            (Tip::Table, Tip::Id),
            (Tip::Table, Tip::TxHash),
            (Tip::Table, Tip::Created),
        ])
        .from(Tip::Table)
        .and_where(Expr::col(Tip::State).eq(0))
        .build_sqlx(PostgresQueryBuilder);
    info!("start check_tip_tx task");
    loop {
        interval.tick().await;
        #[allow(clippy::type_complexity)]
        let rows: Option<Vec<(i32, Option<String>, DateTime<Local>)>> =
            query_as_with(&sql, values.clone())
                .fetch_all(&state.db)
                .await
                .map_err(|e| {
                    error!("{e}");
                    e
                })
                .ok();
        if let Some(rows) = rows {
            for (id, tx_hash, created) in rows {
                if let Some(tx_hash) = tx_hash {
                    let tx_status = get_tx_status(&state.ckb_client, &tx_hash).await;
                    if let Ok(tx_status) = tx_status {
                        debug!("tip id {id} tx {tx_hash} status: {tx_status:?}");
                        match tx_status {
                            ckb_jsonrpc_types::Status::Committed => {
                                let (sql, values) = sea_query::Query::update()
                                    .table(Tip::Table)
                                    .value(Tip::State, 1)
                                    .and_where(Expr::col(Tip::Id).eq(id))
                                    .build_sqlx(PostgresQueryBuilder);
                                sqlx::query_with(&sql, values).execute(&state.db).await.ok();
                                debug!("tip id {} tx {} confirmed", id, tx_hash);
                            }
                            ckb_jsonrpc_types::Status::Pending => {}
                            ckb_jsonrpc_types::Status::Proposed => {}
                            ckb_jsonrpc_types::Status::Unknown => {
                                if (chrono::Local::now() - created) > chrono::Duration::minutes(3) {
                                    let (sql, values) = sea_query::Query::update()
                                        .table(Tip::Table)
                                        .value(Tip::State, 2)
                                        .and_where(Expr::col(Tip::Id).eq(id))
                                        .build_sqlx(PostgresQueryBuilder);
                                    sqlx::query_with(&sql, values).execute(&state.db).await.ok();
                                    debug!("tip id {} tx {} marked as timeout", id, tx_hash);
                                }
                            }
                            ckb_jsonrpc_types::Status::Rejected => {
                                let (sql, values) = sea_query::Query::update()
                                    .table(Tip::Table)
                                    .value(Tip::State, 3)
                                    .and_where(Expr::col(Tip::Id).eq(id))
                                    .build_sqlx(PostgresQueryBuilder);
                                sqlx::query_with(&sql, values).execute(&state.db).await.ok();
                                debug!("tip id {} tx {} marked as rejected", id, tx_hash);
                            }
                        }
                    }
                }
            }
        }
    }
}

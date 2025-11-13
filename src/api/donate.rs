use color_eyre::eyre::eyre;
use common_x::restful::axum::{Json, extract::State, response::IntoResponse};
use common_x::restful::ok;
use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;
use validator::Validate;

use crate::api::build_author;
use crate::indexer::did_document;
use crate::lexicon::tip::{Tip, TipCategory, TipRow, TipState, TipView};
use crate::micro_pay;
use crate::{AppView, error::AppError};

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct DonateParams {
    pub nsid: String,
    pub ckb_addr: String,
    pub sender: String,
    pub amount: String,
}

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct DonateBody {
    pub params: DonateParams,
    pub did: String,
    #[validate(length(equal = 57))]
    pub signing_key_did: String,
    pub signed_bytes: String,
}

#[utoipa::path(post, path = "/api/donate/prepare")]
pub(crate) async fn prepare(
    State(state): State<AppView>,
    Json(body): Json<DonateBody>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    validate_signed(&state.indexer, &body).await?;

    let mut tip_row = TipRow {
        id: -1,
        category: TipCategory::Donate as i32,
        sender_did: body.did.clone(),
        sender: body.params.sender.clone(),
        receiver: body.params.ckb_addr.clone(),
        receiver_did: body.params.ckb_addr.clone(),
        amount: body.params.amount.parse::<i64>()?,
        info: format!("{}/{}", body.params.nsid, body.params.ckb_addr),
        for_uri: body.params.ckb_addr.clone(),
        state: TipState::Prepared as i32,
        tx_hash: None,
        updated: chrono::Local::now(),
        created: chrono::Local::now(),
    };

    let result = micro_pay::payment_prepare(
        &state.pay_url,
        &json!({
            "sender": &tip_row.sender,
            "senderDid": &tip_row.sender_did,
            "receiver": &tip_row.receiver,
            "receiverDid": &tip_row.receiver_did,
            "amount": &tip_row.amount,
            "info": &tip_row.info,
            "splitReceivers": []
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
        category: tip_row.category.to_string(),
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

#[utoipa::path(post, path = "/api/donate/transfer")]
pub(crate) async fn transfer(
    State(state): State<AppView>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let result = micro_pay::payment_transfer(&state.pay_url, &body).await?;
    Ok(ok(result))
}

async fn validate_signed(indexer: &str, body: &DonateBody) -> Result<(), AppError> {
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

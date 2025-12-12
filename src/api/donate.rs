use common_x::restful::axum::{Json, extract::State, response::IntoResponse};
use common_x::restful::ok;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;
use validator::Validate;

use crate::api::{SignedBody, SignedParam, build_author};
use crate::lexicon::tip::{TipCategory, TipRow, TipState, TipView};
use crate::micro_pay;
use crate::{AppView, error::AppError};

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct DonateParams {
    pub nsid: String,
    pub ckb_addr: String,
    pub sender: String,
    pub amount: String,
    pub timestamp: i64,
}

impl SignedParam for DonateParams {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[utoipa::path(post, path = "/api/donate/prepare")]
pub(crate) async fn prepare(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<DonateParams>>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    body.verify_signature(&state.indexer)
        .await
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let mut tip_row = TipRow {
        id: -1,
        category: TipCategory::Donate as i32,
        sender_did: body.did.clone(),
        sender: body.params.sender.clone(),
        receiver: body.params.ckb_addr.clone(),
        receiver_did: body.params.ckb_addr.clone(),
        amount: body.params.amount.parse::<i64>()?,
        info: format!("{}/{}", body.params.nsid, body.params.ckb_addr),
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
            "category": &tip_row.category,
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

#[utoipa::path(post, path = "/api/donate/transfer")]
pub(crate) async fn transfer(
    State(state): State<AppView>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    let result = micro_pay::payment_transfer(&state.pay_url, &body).await?;
    Ok(ok(result))
}

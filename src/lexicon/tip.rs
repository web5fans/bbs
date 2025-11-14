use chrono::{DateTime, Local};
use serde::Serialize;
use serde_json::Value;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum TipState {
    Prepared = 0,
    Committed = 1,
    Timeout = 2,
    Rejected = 3,
}

#[derive(Debug, Clone, Copy)]
pub enum TipCategory {
    Tip = 0,
    Donate = 1,
}

#[derive(sqlx::FromRow, Debug, Serialize)]
#[allow(dead_code)]
pub struct TipRow {
    pub id: i32,
    pub category: i32,
    pub sender: String,
    pub sender_did: String,
    pub receiver: String,
    pub receiver_did: String,
    pub amount: i64,
    pub info: String,
    pub state: i32,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct TipView {
    pub id: String,
    pub category: String,
    pub sender: String,
    pub sender_did: String,
    pub sender_author: Value,
    pub receiver: String,
    pub receiver_did: String,
    pub amount: String,
    pub info: String,
    pub state: String,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct TipDetailView {
    pub id: String,
    pub category: String,
    pub sender: String,
    pub sender_did: String,
    pub sender_author: Value,
    pub receiver: String,
    pub receiver_did: String,
    pub receiver_author: Value,
    pub amount: String,
    pub info: String,
    pub source: Value,
    pub state: String,
    pub tx_hash: Option<String>,
    pub updated: DateTime<Local>,
    pub created: DateTime<Local>,
}

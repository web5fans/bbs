use common_x::log::LogConfig;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct AppConfig {
    pub log_config: LogConfig,
    pub port: u16,
    pub db_url: String,
    pub pds: String,
    pub relayer: String,
    pub bbs_ckb_addr: String,
    pub pay_url: String,
    pub indexer: String,
    pub ckb_url: String,
    pub ckb_net: ckb_sdk::NetworkType,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            log_config: Default::default(),
            port: 8080,
            db_url: Default::default(),
            pds: Default::default(),
            relayer: Default::default(),
            ckb_url: Default::default(),
            bbs_ckb_addr: Default::default(),
            pay_url: Default::default(),
            indexer: Default::default(),
            ckb_net: ckb_sdk::NetworkType::Testnet,
        }
    }
}

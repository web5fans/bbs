use ckb_sdk::{CkbRpcAsyncClient, NetworkType};
use color_eyre::{
    Result,
    eyre::{OptionExt, eyre},
};

pub async fn get_ckb_addr_by_did(
    ckb_client: &CkbRpcAsyncClient,
    ckb_net: &NetworkType,
    did: &str,
) -> Result<String> {
    let did = did.trim_start_matches("did:web5:");
    let did = did.trim_start_matches("did:ckb:");
    let did = did.trim_start_matches("did:plc:");
    let code_hash = match ckb_net {
        NetworkType::Mainnet => "f5f8d0fb3b3f1e0e8f7c9f1c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9fa0b1c2d3",
        NetworkType::Testnet | NetworkType::Dev | NetworkType::Staging | NetworkType::Preview => {
            "510150477b10d6ab551a509b71265f3164e9fd4137fcb5a4322f49f03092c7c5"
        }
    };
    let r = ckb_client
        .get_cells(
            ckb_sdk::rpc::ckb_indexer::SearchKey {
                script: ckb_jsonrpc_types::Script {
                    code_hash: ckb_types::H256(hex::decode(code_hash).unwrap().try_into().unwrap()),
                    hash_type: ckb_jsonrpc_types::ScriptHashType::Type,
                    args: ckb_jsonrpc_types::JsonBytes::from_vec(
                        base32::decode(base32::Alphabet::Rfc4648Lower { padding: false }, did)
                            .ok_or_eyre("did format is invalid")?,
                    ),
                },
                script_type: ckb_sdk::rpc::ckb_indexer::ScriptType::Type,
                script_search_mode: None,
                filter: None,
                with_data: None,
                group_by_transaction: None,
            },
            ckb_sdk::rpc::ckb_indexer::Order::Asc,
            1.into(),
            None,
        )
        .await?;
    let output: &ckb_jsonrpc_types::CellOutput = &r.objects.first().ok_or_eyre("Not Found")?.output;
    let script: ckb_types::packed::Script = output.lock.clone().into();
    let ckb_addr = ckb_sdk::Address::new(*ckb_net, script.into(), true);
    Ok(ckb_addr.to_string())
}

#[allow(dead_code)]
pub async fn get_tx_status(
    ckb_client: &CkbRpcAsyncClient,
    tx_hash: &str,
) -> Result<ckb_jsonrpc_types::Status> {
    let tx_hash: [u8; 32] = hex::decode(tx_hash.strip_prefix("0x").unwrap_or(tx_hash))?
        .try_into()
        .map_err(|_| eyre!("invalid tx_hash format"))?;
    let tx_status = ckb_client.get_transaction(ckb_types::H256(tx_hash)).await?;
    tx_status
        .ok_or_eyre("get tx error")
        .map(|t| t.tx_status.status)
}

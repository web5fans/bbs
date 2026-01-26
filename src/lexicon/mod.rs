use color_eyre::{Result, eyre::OptionExt};

pub(crate) mod administrator;
pub(crate) mod comment;
pub(crate) mod like;
pub(crate) mod notify;
pub(crate) mod operation;
pub(crate) mod post;
pub(crate) mod profile;
pub(crate) mod reply;
pub(crate) mod section;
pub(crate) mod status;
pub(crate) mod tip;
pub(crate) mod whitelist;

pub fn resolve_uri(uri: &str) -> Result<(&str, &str, &str)> {
    let uri_split = uri.split('/').collect::<Vec<&str>>();
    let did = uri_split.get(2).ok_or_eyre("uri format error")?;
    let nsid = uri_split.get(3).ok_or_eyre("uri format error")?;
    let rkey = uri_split.get(4).ok_or_eyre("uri format error")?;
    Ok((did, nsid, rkey))
}

pub fn _resolve_client_uri(client_uri: &str) -> Result<(String, String, String, String)> {
    let uri = client_uri
        .replace("at_", "at://")
        .replace('_', ":")
        .replace('-', "/");
    let uri_split = uri.split('/').collect::<Vec<&str>>();
    let did = uri_split.get(2).ok_or_eyre("uri format error")?.to_string();
    let nsid = uri_split.get(3).ok_or_eyre("uri format error")?.to_string();
    let rkey = uri_split.get(4).ok_or_eyre("uri format error")?.to_string();
    Ok((uri, did, nsid, rkey))
}

#[test]
fn uri() {
    let uri = "at://did:ckb:52vmubyl4y3al5k246owb7nhkmwhwgx7/app.bbs.post/3mbnwjdssbc27";
    let client_uri = uri
        .replace("at://", "at_")
        .replace(':', "_")
        .replace('/', "-");
    let uri = client_uri
        .replace("at_", "at://")
        .replace('_', ":")
        .replace('-', "/");
    println!("client_uri: {client_uri}");
    println!("uri: {uri}");
}

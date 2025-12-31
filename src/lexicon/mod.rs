use color_eyre::{Result, eyre::OptionExt};

pub(crate) mod administrator;
pub(crate) mod comment;
pub(crate) mod like;
pub(crate) mod notify;
pub(crate) mod post;
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

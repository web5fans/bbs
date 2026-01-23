use atrium_api::com::atproto::sync::subscribe_repos::Commit;
use atrium_repo::{Repository, blockstore::CarStore};
use color_eyre::Result;
use serde_json::Value;
use sqlx::{Executor, query};

use crate::{
    AppView,
    atproto::{NSID_COMMENT, NSID_LIKE, NSID_POST, NSID_PROFILE, NSID_REPLY},
    lexicon::{comment::Comment, like::Like, post::Post, profile::Profile, reply::Reply},
    relayer::subscription::CommitHandler,
};

pub(crate) mod stream;
pub(crate) mod subscription;

impl CommitHandler for AppView {
    async fn handle_commit(&self, commit: &Commit) -> Result<()> {
        debug!("Commit: {:?}", commit.commit);

        let mut repo = Repository::open(
            CarStore::open(std::io::Cursor::new(commit.blocks.as_slice())).await?,
            commit.commit.0,
        )
        .await?;

        let mut profile_to_delete = vec![];
        let mut post_to_delete = vec![];
        let mut comment_to_delete = vec![];
        let mut reply_to_delete = vec![];
        let mut like_to_delete = vec![];

        for op in &commit.ops {
            info!("Operation: {:?}", op);
            match op.action.as_str() {
                "create" | "update" | "delete" => (),
                _ => continue,
            }
            let mut s = op.path.split('/');
            let collection = s.next().expect("op.path is empty");

            let repo_str = commit.repo.as_str();
            let uri = format!("at://{}/{}", repo_str, op.path);
            if let Ok(Some(record)) = repo.get_raw::<Value>(&op.path).await {
                debug!("Record: {:?}", record);
                match collection {
                    NSID_PROFILE => match op.action.as_str() {
                        "create" | "update" => {
                            let cid =
                                format!("{}", op.cid.clone().map(|cid| cid.0).unwrap_or_default());
                            info!("{} profile: {:?}", op.action, &record);
                            Profile::insert(&self.db, repo_str, &record, &uri, &cid)
                                .await
                                .map_err(|e| error!("Profile::insert failed: {e}"))
                                .ok();
                        }
                        "delete" => {
                            profile_to_delete.push(uri.clone());
                            info!("Marked profile for deletion: {}", uri);
                        }
                        _ => continue,
                    },
                    NSID_POST => match op.action.as_str() {
                        "create" | "update" => {
                            let cid =
                                format!("{}", op.cid.clone().map(|cid| cid.0).unwrap_or_default());
                            info!("{} post: {:?}", op.action, &record);
                            Post::insert(&self.db, repo_str, &record, &uri, &cid)
                                .await
                                .map_err(|e| error!("Post::insert failed: {e}"))
                                .ok();
                        }
                        "delete" => {
                            post_to_delete.push(uri.clone());
                            info!("Marked post for deletion: {}", uri);
                        }
                        _ => continue,
                    },
                    NSID_COMMENT => match op.action.as_str() {
                        "create" | "update" => {
                            let cid =
                                format!("{}", op.cid.clone().map(|cid| cid.0).unwrap_or_default());
                            info!("{} comment: {:?}", op.action, &record);
                            Comment::insert(&self.db, repo_str, &record, &uri, &cid)
                                .await
                                .map_err(|e| error!("Comment::insert failed: {e}"))
                                .ok();
                        }
                        "delete" => {
                            comment_to_delete.push(uri.clone());
                            info!("Marked comment for deletion: {}", uri);
                        }
                        _ => continue,
                    },
                    NSID_REPLY => match op.action.as_str() {
                        "create" | "update" => {
                            let cid =
                                format!("{}", op.cid.clone().map(|cid| cid.0).unwrap_or_default());
                            info!("{} reply: {:?}", op.action, &record);
                            Reply::insert(&self.db, repo_str, &record, &uri, &cid)
                                .await
                                .map_err(|e| error!("Reply::insert failed: {e}"))
                                .ok();
                        }
                        "delete" => {
                            reply_to_delete.push(uri.clone());
                            info!("Marked reply for deletion: {}", uri);
                        }
                        _ => continue,
                    },
                    NSID_LIKE => match op.action.as_str() {
                        "create" | "update" => {
                            let cid =
                                format!("{}", op.cid.clone().map(|cid| cid.0).unwrap_or_default());
                            info!("{} like: {:?}", op.action, &record);
                            Like::insert(&self.db, repo_str, &record, &uri, &cid)
                                .await
                                .map_err(|e| error!("Like::insert failed: {e}"))
                                .ok();
                        }
                        "delete" => {
                            like_to_delete.push(uri.clone());
                            info!("Marked like for deletion: {}", uri);
                        }
                        _ => continue,
                    },
                    _ => continue,
                }
            } else {
                error!("FAILED: could not find item with operation {}", op.path);
            }
        }

        if !profile_to_delete.is_empty() {
            self.db
                .execute(query(&format!(
                    "DELETE FROM profile WHERE uri IN ({})",
                    post_to_delete
                        .iter()
                        .map(|uri| format!("'{uri}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
                .await
                .map_err(|e| error!("sql execute failed: {e}"))
                .ok();
        }

        if !post_to_delete.is_empty() {
            self.db
                .execute(query(&format!(
                    "DELETE FROM post WHERE uri IN ({})",
                    post_to_delete
                        .iter()
                        .map(|uri| format!("'{uri}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
                .await
                .map_err(|e| error!("sql execute failed: {e}"))
                .ok();
        }

        if !comment_to_delete.is_empty() {
            self.db
                .execute(query(&format!(
                    "DELETE FROM comment WHERE uri IN ({})",
                    comment_to_delete
                        .iter()
                        .map(|uri| format!("'{uri}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
                .await
                .map_err(|e| error!("sql execute failed: {e}"))
                .ok();
        }

        if !reply_to_delete.is_empty() {
            self.db
                .execute(query(&format!(
                    "DELETE FROM reply WHERE uri IN ({})",
                    reply_to_delete
                        .iter()
                        .map(|uri| format!("'{uri}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
                .await
                .map_err(|e| error!("sql execute failed: {e}"))
                .ok();
        }

        if !like_to_delete.is_empty() {
            self.db
                .execute(query(&format!(
                    "DELETE FROM like WHERE uri IN ({})",
                    like_to_delete
                        .iter()
                        .map(|uri| format!("'{uri}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
                .await
                .map_err(|e| error!("sql execute failed: {e}"))
                .ok();
        }

        Ok(())
    }
}

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

            match op.action.as_str() {
                "create" | "update" => {
                    if let Ok(Some(record)) = repo.get_raw::<Value>(&op.path).await {
                        debug!("Record: {:?}", record);
                        let cid =
                            format!("{}", op.cid.clone().map(|cid| cid.0).unwrap_or_default());
                        match collection {
                            NSID_PROFILE => {
                                info!("{} profile", op.action);
                                Profile::insert(&self.db, repo_str, record)
                                    .await
                                    .map_err(|e| error!("Profile::insert failed: {e}"))
                                    .ok();
                            }
                            NSID_POST => {
                                info!("{} post", op.action);
                                Post::insert(&self.db, repo_str, &record, &uri, &cid)
                                    .await
                                    .map_err(|e| error!("Post::insert failed: {e}"))
                                    .ok();
                            }
                            NSID_COMMENT => {
                                info!("{} comment", op.action);
                                Comment::insert(&self.db, repo_str, &record, &uri, &cid)
                                    .await
                                    .map_err(|e| error!("Comment::insert failed: {e}"))
                                    .ok();
                            }
                            NSID_REPLY => {
                                info!("{} reply", op.action);
                                Reply::insert(&self.db, repo_str, &record, &uri, &cid)
                                    .await
                                    .map_err(|e| error!("Reply::insert failed: {e}"))
                                    .ok();
                            }
                            NSID_LIKE => {
                                info!("{} like", op.action);
                                Like::insert(&self.db, repo_str, &record, &uri, &cid)
                                    .await
                                    .map_err(|e| error!("Like::insert failed: {e}"))
                                    .ok();
                            }
                            _ => continue,
                        }
                    } else {
                        error!("FAILED: could not find item with operation {}", op.path);
                    }
                }
                "delete" => match collection {
                    NSID_PROFILE => {
                        profile_to_delete.push(uri.clone());
                        info!("Marked profile for deletion: {}", uri);
                    }
                    NSID_POST => {
                        post_to_delete.push(uri.clone());
                        info!("Marked post for deletion: {}", uri);
                    }
                    NSID_COMMENT => {
                        comment_to_delete.push(uri.clone());
                        info!("Marked comment for deletion: {}", uri);
                    }
                    NSID_REPLY => {
                        reply_to_delete.push(uri.clone());
                        info!("Marked reply for deletion: {}", uri);
                    }
                    NSID_LIKE => {
                        like_to_delete.push(uri.clone());
                        info!("Marked like for deletion: {}", uri);
                    }
                    _ => continue,
                },
                _ => continue,
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

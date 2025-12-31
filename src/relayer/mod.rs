use atrium_api::com::atproto::sync::subscribe_repos::Commit;
use atrium_repo::{Repository, blockstore::CarStore};
use color_eyre::Result;
use serde_json::Value;
use sqlx::{Executor, query};

use crate::{
    AppView, atproto::NSID_POST, lexicon::post::Post, relayer::subscription::CommitHandler,
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

        let mut posts_to_delete = vec![];

        for op in &commit.ops {
            info!("Operation: {:?}", op);
            match op.action.as_str() {
                "create" | "delete" => (),
                _ => continue,
            }
            let mut s = op.path.split('/');
            let collection = s.next().expect("op.path is empty");

            let rkey = s.next().expect("no record key");
            let repo_str = commit.repo.as_str();
            let uri = format!("at://{}/{}", repo_str, op.path);
            if let Ok(Some(record)) = repo.get_raw::<Value>(rkey).await {
                debug!("Record: {:?}", record);
                match collection {
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
                            posts_to_delete.push(uri.clone());
                            info!("Marked post for deletion: {}", uri);
                        }
                        _ => continue,
                    },
                    _ => continue,
                }
            } else {
                error!("FAILED: could not find item with operation {}", op.path);
            }
        }

        if !posts_to_delete.is_empty() {
            self.db
                .execute(query(&format!(
                    "DELETE FROM post WHERE uri IN ({})",
                    posts_to_delete
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

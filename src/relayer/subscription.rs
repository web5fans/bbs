use atrium_api::com::atproto::sync::subscribe_repos::Commit;
use color_eyre::{Result, eyre::eyre};
use futures::StreamExt;
use std::future::Future;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::relayer::stream::Frame;

#[trait_variant::make(HttpService: Send)]
pub trait Subscription {
    async fn next(&mut self) -> Option<Result<Frame>>;
}

pub trait CommitHandler {
    fn handle_commit(&self, commit: &Commit) -> impl Future<Output = Result<()>>;
}

pub(crate) struct RepoSubscription {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl RepoSubscription {
    pub async fn new(relayer: &str) -> Result<Self> {
        let (stream, _) = connect_async(relayer).await?;
        info!("Connected to relayer at {relayer}");
        Ok(RepoSubscription { stream })
    }

    pub async fn run(&mut self, handler: impl CommitHandler) -> Result<()> {
        loop {
            if let Some(message) = self.next().await {
                match message {
                    Ok(Frame::Message(Some(t), message)) => {
                        if t.as_str() == "#commit" {
                            let commit = serde_ipld_dagcbor::from_reader(message.body.as_slice())?;

                            if let Err(err) = handler.handle_commit(&commit).await {
                                error!("FAILED: {err:?}");
                            }
                        }
                    }
                    Ok(Frame::Message(None, _)) | Ok(Frame::Error(_)) => (),
                    Err(e) => {
                        return Err(eyre!("error {e}"));
                    }
                }
            }
        }
    }
}

impl Subscription for RepoSubscription {
    async fn next(&mut self) -> Option<Result<Frame>> {
        match self.stream.next().await {
            Some(Ok(Message::Binary(data))) => Some(Frame::try_from(data.iter().as_slice())),
            Some(Ok(_)) | None => None,
            Some(Err(e)) => Some(Err(eyre!(e))),
        }
    }
}

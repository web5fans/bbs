use atrium_api::com::atproto::sync::subscribe_repos::Commit;
use color_eyre::{Result, eyre::eyre};
use futures::{SinkExt, StreamExt};
use std::future::Future;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Bytes, Message, protocol::WebSocketConfig},
};

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
        let (stream, _) =
            connect_async_with_config(relayer, Some(WebSocketConfig::default()), false).await?;
        info!("Connected to relayer at {relayer}");
        Ok(RepoSubscription { stream })
    }

    pub async fn run(&mut self, handler: impl CommitHandler) -> Result<()> {
        let mut keep_alive_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = keep_alive_interval.tick() => {
                    self.stream.send(Message::Ping(Bytes::new())).await?;
                }
                Some(message) = self.next() => {
                    match message {
                        Ok(Frame::Message(Some(t), message)) => {
                            if t.as_str() == "#commit" {
                                let commit = serde_ipld_dagcbor::from_reader(message.body.as_slice())?;

                                if let Err(err) = handler.handle_commit(&commit).await {
                                    error!("FAILED: {err:?}");
                                }
                            }
                        }
                        Ok(Frame::Message(None, _msg)) => (),
                        Ok(Frame::Error(_e)) => {
                            error!("received error frame");
                            break;
                        }
                        Err(e) => {
                            return Err(eyre!("error {e}"));
                        }
                    }
                }
            }
        }
        Ok(())
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

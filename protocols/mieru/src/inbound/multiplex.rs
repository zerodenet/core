//! Mieru TCP underlay: one authenticated cipher pair, many logical sessions.
pub(crate) mod reader;
pub(crate) mod stream;
pub(crate) mod writer;

use super::{MieruInbound, MieruInboundAcceptedSession, MieruInboundProfile};
use reader::Reader;
use std::sync::{Arc, Mutex};
pub use stream::MieruLogicalStream;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use zero_core::{Error, InboundClientResponse, InboundRouteMultiplexer, SessionAuth};
use zero_traits::AsyncSocket;

pub struct MieruInboundMultiplexer {
    auth: SessionAuth,
    incoming: tokio::sync::Mutex<mpsc::Receiver<MieruLogicalStream>>,
    failure: Arc<Mutex<Option<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl MieruInboundMultiplexer {
    pub(crate) fn from_driver(
        auth: SessionAuth,
        incoming: mpsc::Receiver<MieruLogicalStream>,
        failure: Arc<Mutex<Option<String>>>,
        task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            auth,
            incoming: tokio::sync::Mutex::new(incoming),
            failure,
            task,
        }
    }
}
impl Drop for MieruInboundMultiplexer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl MieruInboundProfile {
    pub async fn accept_multiplexer<S>(
        &self,
        mut socket: S,
    ) -> Result<MieruInboundMultiplexer, Error>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let accept = self.accept_request(&mut socket).await?;
        let id = accept.mieru_session.session_id;
        let (ready, incoming) = mpsc::channel(reader::MAX_SESSIONS);
        let (outgoing, commands) = mpsc::channel(64);
        let (dropped, drop_events) = mpsc::unbounded_channel();
        let mut reader = Reader::new(
            ready.clone(),
            outgoing,
            dropped,
            self.options.receive.clone(),
        );
        reader
            .insert(id, accept.remaining_payload)
            .map_err(|_| Error::Io("mieru initial session dispatch"))?;
        let (read, write) = tokio::io::split(socket);
        let failure = Arc::new(Mutex::new(None));
        let result = failure.clone();
        let writer_pattern = accept.traffic_pattern.clone();
        let writer_username = accept.username.clone();
        let task = tokio::spawn(async move {
            let outcome = tokio::select! {
                result = reader.run(read, accept.client_cipher, drop_events) => result,
                result = writer::run_with_pattern(
                    write,
                    accept.server_cipher,
                    id,
                    commands,
                    writer_pattern,
                    writer_username,
                ) => result,
            };
            if let Err(error) = outcome {
                *result.lock().unwrap() = Some(error.to_string());
            }
            drop(ready);
        });
        Ok(MieruInboundMultiplexer {
            auth: accept.auth,
            incoming: tokio::sync::Mutex::new(incoming),
            failure,
            task,
        })
    }
}
impl InboundRouteMultiplexer for MieruInboundMultiplexer {
    type Incoming = MieruLogicalStream;
    type Route = MieruInboundAcceptedSession<MieruLogicalStream>;
    type ResponseProtocol = MieruInbound;
    type Error = std::io::Error;
    fn auth(&self) -> Option<&SessionAuth> {
        Some(&self.auth)
    }
    fn close(&self, _reason: &str) {
        self.task.abort();
    }
    fn response_protocol(&self) -> MieruInbound {
        MieruInbound
    }
    async fn accept_next(&self) -> Result<Option<Self::Incoming>, Self::Error> {
        let stream = self.incoming.lock().await.recv().await;
        if stream.is_none() {
            if let Some(error) = self.failure.lock().unwrap().as_ref() {
                return Err(std::io::Error::other(error.clone()));
            }
        }
        Ok(stream)
    }
    async fn accept_route(&self, mut stream: Self::Incoming) -> Result<Self::Route, Self::Error> {
        let mut session = crate::tunnel::accept_tunneled_session(&mut stream)
            .await
            .map_err(std::io::Error::other)?;
        session.apply_auth(self.auth.clone());
        Ok(MieruInboundAcceptedSession::from_session_stream(
            session, stream,
        ))
    }
}
impl InboundClientResponse<MieruLogicalStream> for MieruInbound {
    async fn send_ok(&self, _client: &mut MieruLogicalStream) -> Result<(), Error> {
        Ok(())
    }
    async fn send_blocked(&self, client: &mut MieruLogicalStream) -> Result<(), Error> {
        AsyncSocket::shutdown(client)
            .await
            .map_err(|_| Error::Io("mieru close rejected session"))
    }
    async fn send_upstream_failure(&self, client: &mut MieruLogicalStream) -> Result<(), Error> {
        self.send_blocked(client).await
    }
}

//! Response ownership adapts the common relay without another I/O executor.
use crate::runtime::tcp_ingress::InboundProtocol;
use async_trait::async_trait;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    inbound::{AcceptedStream, ReplyOutcome},
    stream::BoxStream,
};

pub(super) struct PendingStream {
    reply: Option<Box<dyn AcceptedStream>>,
    io: Option<BoxStream>,
}
impl PendingStream {
    pub(super) fn new(reply: Box<dyn AcceptedStream>) -> Self {
        Self {
            reply: Some(reply),
            io: None,
        }
    }
    async fn respond(&mut self, outcome: ReplyOutcome) -> Result<(), EngineError> {
        let reply = self.reply.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "inbound response was already consumed",
            )
        })?;
        self.io = reply.respond(outcome).await.map_err(io::Error::other)?;
        if matches!(outcome, ReplyOutcome::LocalChannelReady) && self.io.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "successful inbound response supplied no stream",
            )
            .into());
        }
        Ok(())
    }
    fn stream(&mut self) -> io::Result<&mut BoxStream> {
        self.io.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "inbound stream has not been handed over",
            )
        })
    }
}
impl AsyncRead for PendingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.stream()?.as_mut().poll_read(cx, bytes)
    }
}
impl AsyncWrite for PendingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stream()?.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream()?.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream()?.as_mut().poll_shutdown(cx)
    }
}
pub(super) struct Protocol;
#[async_trait]
impl InboundProtocol for Protocol {
    type ClientStream = PendingStream;
    async fn send_ok(&self, client: &mut PendingStream) -> Result<(), EngineError> {
        client.respond(ReplyOutcome::LocalChannelReady).await
    }
    async fn send_blocked(&self, client: &mut PendingStream) -> Result<(), EngineError> {
        client.respond(ReplyOutcome::Blocked).await
    }
    async fn send_upstream_failure(&self, client: &mut PendingStream) -> Result<(), EngineError> {
        client.respond(ReplyOutcome::UpstreamFailed).await
    }
}

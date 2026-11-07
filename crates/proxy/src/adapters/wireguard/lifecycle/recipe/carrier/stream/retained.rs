//! Ordinary connect drives the same retained cold opening before returning ACK.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::{
    contract::{ChannelError, CloseReason, Impact, Reason, Stage},
    controlled_carrier::loan::{LoanIo, LoanOwner, StreamLoan},
};
pub(super) struct Stream {
    // Actual IO must return before its original delegate controller is dropped.
    io: LoanIo,
    owner: LoanOwner,
}
impl Stream {
    pub(super) fn new(loan: StreamLoan) -> Self {
        Self {
            io: loan.io,
            owner: loan.owner,
        }
    }
    pub(super) async fn establish(&mut self) -> Result<(), ChannelError> {
        // Stream::poll_flush first polls the same ClientTcpOpening until real
        // SYN acknowledgement; no application payload or second worker exists.
        self.io.flush().await.map_err(|source| {
            self.request_retirement();
            let reason = match source.kind() {
                io::ErrorKind::TimedOut => Reason::Timeout,
                io::ErrorKind::WouldBlock => Reason::Capacity,
                io::ErrorKind::InvalidInput => Reason::InvalidConfig,
                io::ErrorKind::BrokenPipe => Reason::Revoked,
                _ => Reason::Io,
            };
            ChannelError::new(Stage::Dial, reason, Impact::Session).with_source(source)
        })
    }
    pub(super) fn request_retirement(&self) {
        self.owner.cancel(CloseReason::Shutdown);
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

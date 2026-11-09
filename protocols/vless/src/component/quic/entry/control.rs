use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, DynChannelControl},
    BoxFuture,
    CloseReason,
};
pub(in crate::component::quic) struct ProtocolControl(pub PacketMetadataArc<Owners>);
impl DynChannelControl for ProtocolControl {
    fn cancel(&self, reason: CloseReason) {
        ChannelControl::cancel(self, reason);
    }
    fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(ChannelControl::wait_closed(self, deadline))
    }
}
impl ChannelControl for ProtocolControl {
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
    }
    async fn wait_closed(&mut self, _: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        let (physical, received) =
            tokio::join!(self.0.close_original(), self.0.receive_actor_original());
        received?;
        let physical = match physical {
            Ok(report) => report,
            Err(source) => {
                self.0.diagnostics.physical(source);
                CloseReport::default()
            }
        };
        if let Some(source) = self.0.diagnostics.pending_failure() {
            return Err(ChannelError::retained(source));
        }
        if !self.0.state.lock().await.cleanup_complete {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "actual QUIC protocol return remains retained".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        Ok(physical)
    }
}
pub(in crate::component::quic) struct LogicalControl(pub PacketMetadataArc<Entry>);
impl DynChannelControl for LogicalControl {
    fn cancel(&self, reason: CloseReason) {
        ChannelControl::cancel(self, reason);
    }
    fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(ChannelControl::wait_closed(self, deadline))
    }
}
impl ChannelControl for LogicalControl {
    fn cancel(&self, _: CloseReason) {
        self.0.retirement.request_retirement();
    }
    async fn wait_closed(&mut self, _: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        loop {
            let returned = self.0.owners.changed.notified();
            tokio::pin!(returned);
            returned.as_mut().enable();
            if self
                .0
                .owners
                .returned
                .load(std::sync::atomic::Ordering::Acquire)
            {
                break;
            }
            returned.await;
        }
        self.0.retirement.request_retirement();
        self.0.retirement.wait_protocol_original().await?;
        Ok(CloseReport::default())
    }
}
/// This exact logical stream keeps its original entry while either split half
/// exists. FIN and stream Drop only request cleanup; neither refunds original N.
pub(in crate::component::quic) struct View {
    pub io: Option<zero_platform_tokio::TcpRelayStream>,
    pub original: PacketMetadataArc<Entry>,
}
impl Drop for View {
    fn drop(&mut self) {
        // Destroy the actual logical stream first, outside all owner state locks.
        // This remains a unique-view return, never a NativeClosed/HostACK receipt.
        drop(self.io.take());
        self.original
            .owners
            .returned
            .store(true, std::sync::atomic::Ordering::Release);
        self.original.owners.changed.notify_waiters();
        self.original.retirement.request_retirement();
    }
}
impl AsyncRead for View {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.io.as_mut().expect("live actual QUIC stream")).poll_read(cx, buf)
    }
}
impl AsyncWrite for View {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.io.as_mut().expect("live actual QUIC stream")).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.io.as_mut().expect("live actual QUIC stream")).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.io.as_mut().expect("live actual QUIC stream")).poll_shutdown(cx)
    }
}

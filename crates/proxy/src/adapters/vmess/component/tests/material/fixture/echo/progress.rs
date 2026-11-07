//! Read-only boundaries at the existing deadline; no extra wait or IO driver.
use super::*;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};
#[derive(Default)]
pub(super) struct Progress {
    pub client: AtomicU8,
    pub target: AtomicU8,
    pub uploaded: AtomicUsize,
    pub received: AtomicUsize,
    pub target_received: AtomicUsize,
    pub target_sent: AtomicUsize,
}
pub(super) struct AtDeadline<'a> {
    pub client: &'a Running,
    pub progress: Arc<Progress>,
    pub port: u16,
}
impl Drop for AtDeadline<'_> {
    fn drop(&mut self) {
        let p = &self.progress;
        eprintln!("actual VMess original deadline boundaries targetPort={} clientPhase={} targetPhase={} clientAcceptedUpload={} clientReadReverse={} targetReadUpload={} targetAcceptedReverse={} flows(up,down,inRx,inTx,outRx,outTx)={:?} faults={:?}",self.port,p.client.load(Ordering::Acquire),p.target.load(Ordering::Acquire),p.uploaded.load(Ordering::Acquire),p.received.load(Ordering::Acquire),p.target_received.load(Ordering::Acquire),p.target_sent.load(Ordering::Acquire),self.client.proxy.engine().active_sessions().iter().map(|flow|(flow.bytes_up,flow.bytes_down,flow.inbound_rx_bytes,flow.inbound_tx_bytes,flow.outbound_rx_bytes,flow.outbound_tx_bytes)).collect::<Vec<_>>(),self.client.proxy.engine().completed_sessions().iter().filter_map(|flow|flow.failure.as_ref()).collect::<Vec<_>>());
    }
}
pub(super) struct Counted<S> {
    pub io: S,
    pub progress: Arc<Progress>,
    pub target: bool,
}
impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Counted<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let result = std::pin::Pin::new(&mut self.io).poll_read(cx, buffer);
        if matches!(&result, std::task::Poll::Ready(Ok(()))) {
            let count = buffer.filled().len() - before;
            let counter = if self.target {
                &self.progress.target_received
            } else {
                &self.progress.received
            };
            counter.fetch_add(count, Ordering::AcqRel);
        }
        result
    }
}
impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Counted<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let result = std::pin::Pin::new(&mut self.io).poll_write(cx, bytes);
        if let std::task::Poll::Ready(Ok(count)) = &result {
            let counter = if self.target {
                &self.progress.target_sent
            } else {
                &self.progress.uploaded
            };
            counter.fetch_add(*count, Ordering::AcqRel);
        }
        result
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

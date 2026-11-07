//! Observe the same read_to_end polls, including when its original deadline wins.
use crate::Proxy;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};
pub(super) struct ReadProgress<'a, R> {
    io: &'a mut R,
    proxy: &'a Proxy,
    bytes: usize,
    eof: bool,
    label: &'static str,
}
impl<'a, R> ReadProgress<'a, R> {
    pub(super) fn new(io: &'a mut R, proxy: &'a Proxy, label: &'static str) -> Self {
        Self {
            io,
            proxy,
            bytes: 0,
            eof: false,
            label,
        }
    }
}
impl<R: AsyncRead + Unpin> AsyncRead for ReadProgress<'_, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = output.filled().len();
        let available = output.remaining();
        let result = Pin::new(&mut *this.io).poll_read(cx, output);
        let received = output.filled().len() - before;
        this.bytes += received;
        if available != 0 && received == 0 && matches!(result, Poll::Ready(Ok(()))) {
            this.eof = true;
        }
        result
    }
}
impl<R> Drop for ReadProgress<'_, R> {
    fn drop(&mut self) {
        flow_boundaries(self.proxy, self.label);
        eprintln!(
            "actual {} bytes={} EOF={} activeFlows={} completedFailures={:?}",
            self.label,
            self.bytes,
            self.eof,
            self.proxy.engine().active_sessions().len(),
            self.proxy
                .engine()
                .completed_sessions()
                .iter()
                .filter_map(|flow| flow.failure.as_ref())
                .collect::<Vec<_>>()
        );
    }
}

/// Same Engine snapshots at timeout, without polling or retaining a flow lease.
pub(super) struct FlowProgress {
    proxy: Proxy,
    label: &'static str,
}
impl FlowProgress {
    pub(super) fn new(proxy: &Proxy, label: &'static str) -> Self {
        Self {
            proxy: proxy.clone(),
            label,
        }
    }
}
impl Drop for FlowProgress {
    fn drop(&mut self) {
        flow_boundaries(&self.proxy, self.label);
    }
}
fn flow_boundaries(proxy: &Proxy, label: &str) {
    let active = proxy.engine().active_sessions();
    let completed = proxy.engine().completed_sessions();
    eprintln!("actual {label} active boundaries (id,port,up,down,inRx,inTx,outRx,outTx)={:?}; completed boundaries={:?}",
        active.iter().map(|flow|(flow.id,flow.port,flow.bytes_up,flow.bytes_down,flow.inbound_rx_bytes,flow.inbound_tx_bytes,flow.outbound_rx_bytes,flow.outbound_tx_bytes)).collect::<Vec<_>>(),
        completed.iter().map(|flow|(flow.id,flow.port,flow.bytes_up,flow.bytes_down,flow.inbound_rx_bytes,flow.inbound_tx_bytes,flow.outbound_rx_bytes,flow.outbound_tx_bytes)).collect::<Vec<_>>());
}

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

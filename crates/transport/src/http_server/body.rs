use bytes::Bytes;
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::mpsc;
pub(super) struct ResponseBody {
    pub(super) receiver: mpsc::Receiver<io::Result<Bytes>>,
    pub(super) failed: Arc<AtomicBool>,
    pub(super) task: tokio::task::AbortHandle,
}
impl hyper::body::Body for ResponseBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, io::Error>>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(None) if self.failed.swap(false, Ordering::AcqRel) => Poll::Ready(Some(
                Err(io::Error::other("HTTP request failed or timed out")),
            )),
            value => value.map(|item| item.map(|result| result.map(hyper::body::Frame::data))),
        }
    }
}
impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.task.abort();
    }
}

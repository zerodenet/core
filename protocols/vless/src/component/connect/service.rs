//! One retained control IO owns its original preparation future. This neither
//! spawns a worker nor treats cancellation/IO return as a native close receipt.
use super::*;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

type Preparation = Pin<Box<dyn Future<Output = Result<BoxStream, ChannelError>> + Send>>;
enum State {
    Preparing(Preparation),
    Ready(BoxStream),
    Failed(Arc<ChannelError>),
    Closed,
}
pub(super) struct PreparingStream {
    // Exclusive polling uses get_mut; no mutex guard survives an await. The
    // mutex only preserves the existing erased stream's possible Sync facade.
    state: Mutex<State>,
}
impl PreparingStream {
    pub(super) fn new(
        prepare: impl Future<Output = Result<BoxStream, ChannelError>> + Send + 'static,
    ) -> Self {
        Self {
            state: Mutex::new(State::Preparing(Box::pin(prepare))),
        }
    }
    fn poll_io(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&mut BoxStream>> {
        let state = self.state.get_mut().expect("exclusive control preparation");
        if let State::Preparing(prepare) = state {
            *state = match prepare.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(io)) => State::Ready(io),
                Poll::Ready(Err(source)) => State::Failed(Arc::new(source)),
            };
        }
        match state {
            State::Ready(io) => Poll::Ready(Ok(io)),
            State::Failed(source) => Poll::Ready(Err(io::Error::other(source.clone()))),
            State::Closed => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            State::Preparing(_) => unreachable!("preparation was polled above"),
        }
    }
}
impl AsyncRead for PreparingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_io(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
            Poll::Ready(Ok(io)) => io.as_mut().poll_read(cx, bytes),
        }
    }
}
impl AsyncWrite for PreparingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_io(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
            Poll::Ready(Ok(io)) => io.as_mut().poll_write(cx, bytes),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_io(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
            Poll::Ready(Ok(io)) => io.as_mut().poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let state = self.state.get_mut().expect("exclusive control preparation");
        if let State::Ready(io) = state {
            return io.as_mut().poll_shutdown(cx);
        }
        // A stop before authentication must not start an Rvs request merely
        // to close it. Its retained LoanOwner still owes explicit settlement.
        *state = State::Closed;
        Poll::Ready(Ok(()))
    }
}

use super::{budget::Lease, client::Life, *};
use crate::grpc::GrpcStream;
use std::{
    error::Error,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct NativeGrpcStream {
    inner: GrpcStream,
    _lease: Lease,
    pub(super) life: Arc<Life>,
    failure: Mutex<Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send>>>>,
}
#[derive(Debug)]
pub(super) struct SharedFailure(pub(super) Arc<ChannelError>);
impl fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for SharedFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl NativeGrpcStream {
    pub(super) fn new(inner: GrpcStream, lease: Lease, life: Arc<Life>) -> Self {
        Self {
            inner,
            _lease: lease,
            life,
            failure: Mutex::new(None),
        }
    }
    fn driver_failure(
        &mut self,
        cx: &mut Context<'_>,
        fallback: Option<io::Result<()>>,
    ) -> Poll<io::Result<()>> {
        let failure = self.failure.get_mut().unwrap();
        if let Some(fallback) = fallback {
            let witness = self.life.physical.witness.lock().unwrap().clone();
            let Some(witness) = witness else {
                return Poll::Ready(fallback);
            };
            *failure = Some(Box::pin(async move {
                match witness.wait().await.result() {
                    Ok(()) => fallback,
                    Err(source) => Err(io::Error::new(
                        fallback
                            .err()
                            .map_or(io::ErrorKind::Other, |error| error.kind()),
                        SharedFailure(source),
                    )),
                }
            }));
        }
        let result = std::task::ready!(failure
            .as_mut()
            .expect("native gRPC driver failure")
            .as_mut()
            .poll(cx));
        *failure = None;
        Poll::Ready(result)
    }
}
impl AsyncRead for NativeGrpcStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.failure.get_mut().unwrap().is_some() {
            return self.driver_failure(cx, None);
        }
        let before = buffer.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buffer) {
            Poll::Ready(result)
                if self
                    .life
                    .physical
                    .done
                    .load(std::sync::atomic::Ordering::Acquire)
                    && (result.is_err() || buffer.filled().len() == before) =>
            {
                self.driver_failure(cx, Some(result))
            }
            result => result,
        }
    }
}
impl AsyncWrite for NativeGrpcStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.failure.get_mut().unwrap().is_some() {
            return self
                .driver_failure(cx, None)
                .map(|result| result.map(|()| 0));
        }
        match Pin::new(&mut self.inner).poll_write(cx, data) {
            Poll::Ready(Err(error))
                if self
                    .life
                    .physical
                    .done
                    .load(std::sync::atomic::Ordering::Acquire) =>
            {
                self.driver_failure(cx, Some(Err(error)))
                    .map(|result| result.map(|()| 0))
            }
            result => result,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.failure.get_mut().unwrap().is_some() {
            return self.driver_failure(cx, None);
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Err(error))
                if self
                    .life
                    .physical
                    .done
                    .load(std::sync::atomic::Ordering::Acquire) =>
            {
                self.driver_failure(cx, Some(Err(error)))
            }
            result => result,
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.failure.get_mut().unwrap().is_some() {
            return self.driver_failure(cx, None);
        }
        match Pin::new(&mut self.inner).poll_shutdown(cx) {
            Poll::Ready(Err(error))
                if self
                    .life
                    .physical
                    .done
                    .load(std::sync::atomic::Ordering::Acquire) =>
            {
                self.driver_failure(cx, Some(Err(error)))
            }
            result => result,
        }
    }
}

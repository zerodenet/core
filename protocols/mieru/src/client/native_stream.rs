//! Native logical errors wait for the exact driver join and retain its failure Arc.
use crate::inbound::multiplex::stream::MieruLogicalStream;
use std::{
    error::Error,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_core::component::SessionId;
use zero_platform_tokio::contract::{scope::TaskJoinWitness, ChannelError};

pub struct NativeTcpStream {
    stream: MieruLogicalStream,
    join: TaskJoinWitness,
    driver: tokio::sync::mpsc::Sender<super::OpenRequest>,
    error: Option<Pin<Box<dyn Future<Output = io::Error> + Send>>>,
}
#[derive(Debug)]
struct DriverFailure(Arc<ChannelError>);
impl fmt::Display for DriverFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for DriverFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl NativeTcpStream {
    pub(super) fn new(
        stream: MieruLogicalStream,
        join: TaskJoinWitness,
        driver: tokio::sync::mpsc::Sender<super::OpenRequest>,
    ) -> Self {
        Self {
            stream,
            join,
            driver,
            error: None,
        }
    }
    fn failed(&mut self, cx: &mut Context<'_>, fallback: Option<io::Error>) -> Poll<io::Error> {
        if let Some(fallback) = fallback {
            // A rejected/closed logical session does not end the shared driver.
            // Only its actual command-receiver closure permits awaiting its join.
            if !self.driver.is_closed() {
                return Poll::Ready(fallback);
            }
            let join = self.join.clone();
            self.error = Some(Box::pin(async move {
                match join.wait().await.result() {
                    Ok(()) => fallback,
                    Err(source) => io::Error::new(fallback.kind(), DriverFailure(source)),
                }
            }));
        }
        let error = std::task::ready!(self
            .error
            .as_mut()
            .expect("pending native driver error")
            .as_mut()
            .poll(cx));
        self.error = None;
        Poll::Ready(error)
    }
    pub(crate) fn driver_failure(
        source: &(dyn Error + 'static),
        session: Option<SessionId>,
    ) -> Option<ChannelError> {
        let mut current = Some(source);
        while let Some(error) = current {
            // io::Error::source forwards the payload's source, so its
            // DriverFailure wrapper is retained in get_ref, not yielded as a
            // chain node. Preserve its exact original receipt Arc.
            let driver = error.downcast_ref::<DriverFailure>().or_else(|| {
                error
                    .downcast_ref::<io::Error>()?
                    .get_ref()?
                    .downcast_ref::<DriverFailure>()
            });
            if let Some(driver) = driver {
                let source = driver.0.clone();
                let mut result = ChannelError::new(source.stage, source.reason, source.impact);
                result.component = source.component;
                result.session = session.or(source.session);
                result.delivery = source.delivery;
                result.retry = source.retry;
                return Some(result.with_source(DriverFailure(source)));
            }
            current = error.source();
        }
        None
    }
}
impl AsyncRead for NativeTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.error.is_some() {
            return self.failed(cx, None).map(Err);
        }
        match Pin::new(&mut self.stream).poll_read(cx, buffer) {
            Poll::Ready(Err(error)) => self.failed(cx, Some(error)).map(Err),
            result => result,
        }
    }
}
impl AsyncWrite for NativeTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.error.is_some() {
            return self.failed(cx, None).map(Err);
        }
        match Pin::new(&mut self.stream).poll_write(cx, bytes) {
            Poll::Ready(Err(error)) => self.failed(cx, Some(error)).map(Err),
            result => result,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.error.is_some() {
            return self.failed(cx, None).map(Err);
        }
        match Pin::new(&mut self.stream).poll_flush(cx) {
            Poll::Ready(Err(error)) => self.failed(cx, Some(error)).map(Err),
            result => result,
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.error.is_some() {
            return self.failed(cx, None).map(Err);
        }
        match Pin::new(&mut self.stream).poll_shutdown(cx) {
            Poll::Ready(Err(error)) => self.failed(cx, Some(error)).map(Err),
            result => result,
        }
    }
}

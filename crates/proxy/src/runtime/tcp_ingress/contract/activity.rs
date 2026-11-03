use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(test)]
#[path = "activity/tests.rs"]
mod tests;

pub(crate) use zero_platform_tokio::contract::inbound::StreamActivity as TcpRelayActivity;

pub(crate) struct TcpActivityStream<S> {
    inner: S,
    activity: TcpRelayActivity,
}

impl<S> TcpActivityStream<S> {
    pub(crate) fn new(inner: S, activity: TcpRelayActivity) -> Self {
        Self { inner, activity }
    }
}

impl<S> AsyncRead for TcpActivityStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled_before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(()))) && buffer.filled().len() > filled_before {
            self.activity.touch();
        }
        result
    }
}

impl<S> AsyncWrite for TcpActivityStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(written)) if *written != 0) {
            self.activity.touch();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

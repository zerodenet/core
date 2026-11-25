//! Logical QUIC stream loan wrapper over [`QuicPooledStream`].

use super::{QuicPoolProtocol, QuicPooledStream};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl},
        BoxFuture, CancellationToken, ChannelError, CloseReason,
    },
    controlled_carrier::loan::StreamLoan,
};

struct SharedLoan<P: QuicPoolProtocol> {
    stream: Mutex<Option<QuicPooledStream<P>>>,
    stop: CancellationToken,
}

struct LoanView<P: QuicPoolProtocol>(Arc<SharedLoan<P>>);
struct LoanControl<P: QuicPoolProtocol>(Arc<SharedLoan<P>>);

pub(super) fn loan_stream<P: QuicPoolProtocol>(stream: QuicPooledStream<P>) -> StreamLoan {
    let original = stream.original.clone();
    let shared = Arc::new(SharedLoan {
        stream: Mutex::new(Some(stream)),
        stop: CancellationToken::new(),
    });
    StreamLoan::new(
        Box::pin(LoanView(shared.clone())),
        Box::new(LoanControl(shared)).into(),
        original,
    )
}

impl<P: QuicPoolProtocol> LoanView<P> {
    fn check(&self) -> io::Result<()> {
        if self.0.stop.is_cancelled() {
            Err(io::ErrorKind::BrokenPipe.into())
        } else {
            Ok(())
        }
    }
}

impl<P: QuicPoolProtocol> AsyncRead for LoanView<P> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check()?;
        Pin::new(
            self.0
                .stream
                .lock()
                .unwrap()
                .as_mut()
                .expect("live QUIC stream loan"),
        )
        .poll_read(cx, bytes)
    }
}

impl<P: QuicPoolProtocol> AsyncWrite for LoanView<P> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check()?;
        Pin::new(
            self.0
                .stream
                .lock()
                .unwrap()
                .as_mut()
                .expect("live QUIC stream loan"),
        )
        .poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check()?;
        Pin::new(
            self.0
                .stream
                .lock()
                .unwrap()
                .as_mut()
                .expect("live QUIC stream loan"),
        )
        .poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check()?;
        Pin::new(
            self.0
                .stream
                .lock()
                .unwrap()
                .as_mut()
                .expect("live QUIC stream loan"),
        )
        .poll_shutdown(cx)
    }
}

impl<P: QuicPoolProtocol> DynChannelControl for LoanControl<P> {
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
    }

    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.stop.cancel();
            let mut retained = self.0.stream.lock().unwrap();
            let Some(stream) = retained.as_mut() else {
                return Ok(CloseReport::default());
            };
            stream.io.retire_logical();
            drop(retained.take());
            Ok(CloseReport {
                released_resources: vec![format!(
                    "{} original logical QUIC stream returned",
                    P::DISPLAY_NAME
                )],
                ..Default::default()
            })
        })
    }
}

impl<P: QuicPoolProtocol> Drop for LoanControl<P> {
    fn drop(&mut self) {
        self.0.stop.cancel();
    }
}

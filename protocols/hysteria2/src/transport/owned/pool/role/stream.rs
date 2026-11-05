//! One logical QUIC stream loan retains its actual authentication and I/O.
use super::Stream;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl},
        BoxFuture, CancellationToken, ChannelError, CloseReason,
    },
    controlled_carrier::loan::StreamLoan,
};

struct Shared {
    stream: Mutex<Option<Stream>>,
    stop: CancellationToken,
}
struct View(Arc<Shared>);
struct Control(Arc<Shared>);

pub(super) fn loan(stream: Stream) -> StreamLoan {
    let original = stream.original.clone();
    let shared = Arc::new(Shared {
        stream: Mutex::new(Some(stream)),
        stop: CancellationToken::new(),
    });
    StreamLoan::new(
        Box::pin(View(shared.clone())),
        Box::new(Control(shared)),
        original,
    )
}
impl View {
    fn check(&self) -> io::Result<()> {
        if self.0.stop.is_cancelled() {
            Err(io::ErrorKind::BrokenPipe.into())
        } else {
            Ok(())
        }
    }
}
impl AsyncRead for View {
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
                .expect("live HY2 loan"),
        )
        .poll_read(cx, bytes)
    }
}
impl AsyncWrite for View {
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
                .expect("live HY2 loan"),
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
                .expect("live HY2 loan"),
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
                .expect("live HY2 loan"),
        )
        .poll_shutdown(cx)
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
    }
    fn wait_closed(
        &mut self,
        _: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.stop.cancel();
            let mut retained = self.0.stream.lock().unwrap();
            let Some(stream) = retained.as_mut() else {
                return Ok(CloseReport::default());
            };
            // LoanOwner first confirms the unique transferred view returned.
            // Retire these same two logical QUIC directions, then release their
            // sealed users. The pool still owns native and driver completion.
            stream.io.retire_logical();
            drop(retained.take());
            Ok(CloseReport {
                released_resources: vec!["HY2 original logical QUIC stream returned".into()],
                ..Default::default()
            })
        })
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.0.stop.cancel();
    }
}

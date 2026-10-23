use crate::contract_support as support;
use std::{
    cell::Cell,
    io,
    marker::PhantomPinned,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::contract::{context::OpenContext, stream::*, ChannelError};

/// Deliberately !Unpin and !Sync. Interior state needs no pin projection or unsafe.
struct PinnedStream {
    read: Cell<bool>,
    writes: Arc<CellCounter>,
    _pin: PhantomPinned,
}
struct CellCounter {
    closed: AtomicBool,
}
impl AsyncRead for PinnedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let state = self.as_ref().get_ref();
        if buffer.remaining() != 0 && !state.read.replace(true) {
            buffer.put_slice(b"x");
        }
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for PinnedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.as_ref()
            .get_ref()
            .writes
            .closed
            .store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}
struct Outbound(Arc<CellCounter>);
impl StreamOutbound for Outbound {
    type Stream = PinnedStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<Self::Stream>, ChannelError> {
        assert_eq!(request.destination, support::destination());
        assert_eq!(context.business_facts()?.session.0, 1);
        Ok(Connected {
            io: PinnedStream {
                read: Cell::new(false),
                writes: self.0.clone(),
                _pin: PhantomPinned,
            },
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}

#[tokio::test]
async fn erasure_supports_pinned_non_sync_stream_and_preserves_deferred_confirmation() {
    let observed = Arc::new(CellCounter {
        closed: AtomicBool::new(false),
    });
    let outbound = erase_stream(Outbound(observed.clone()));
    let mut connected = outbound
        .connect(
            ConnectRequest {
                destination: support::destination(),
            },
            support::context(),
        )
        .await
        .unwrap();
    assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
    let mut bytes = Vec::new();
    connected.io.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"x");
    connected.io.write_all(b"after read EOF").await.unwrap();
    connected.io.flush().await.unwrap();
    connected.io.shutdown().await.unwrap();
    assert!(observed.closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn erased_duplex_preserves_half_close_and_the_reverse_response() {
    let (local, mut remote) = tokio::io::duplex(32);
    let mut local: BoxStream = Box::pin(local);
    local.write_all(b"request").await.unwrap();
    local.shutdown().await.unwrap();
    let mut request = Vec::new();
    remote.read_to_end(&mut request).await.unwrap();
    assert_eq!(request, b"request");
    remote.write_all(b"response after EOF").await.unwrap();
    remote.shutdown().await.unwrap();
    let mut response = Vec::new();
    local.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, b"response after EOF");
}

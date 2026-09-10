use super::super::fixtures::{echo, profile};
use mieru::client::{ClientConnection, PoolKey};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::TcpRelayStream;

struct WriteGate {
    inner: tokio::io::DuplexStream,
    blocked: Arc<AtomicBool>,
}

impl AsyncRead for WriteGate {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl AsyncWrite for WriteGate {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.blocked.load(Ordering::Acquire) {
            Poll::Pending
        } else {
            Pin::new(&mut self.inner).poll_write(cx, buffer)
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) fn key(user: &str, password: &str) -> PoolKey {
    PoolKey::new("leaf", "test", 1, user, password, false)
}

pub(super) async fn connect(
    count: Arc<AtomicUsize>,
    user: &str,
    password: &str,
) -> std::io::Result<Arc<ClientConnection>> {
    count.fetch_add(1, Ordering::SeqCst);
    establish(user, password).await
}

pub(super) async fn establish(
    user: &str,
    password: &str,
) -> std::io::Result<Arc<ClientConnection>> {
    let (client, server) = tokio::io::duplex(65536);
    tokio::spawn(async move {
        echo(
            profile()
                .accept_multiplexer(TcpRelayStream::new(server))
                .await
                .unwrap(),
        )
        .await;
    });
    ClientConnection::tcp(TcpRelayStream::new(client), user, password).await
}

pub(super) async fn establish_with_server_write_gate() -> (
    Arc<ClientConnection>,
    Arc<AtomicBool>,
    tokio::task::JoinHandle<()>,
) {
    let (client, server) = tokio::io::duplex(65536);
    let blocked = Arc::new(AtomicBool::new(false));
    let server_blocked = blocked.clone();
    let server = tokio::spawn(async move {
        echo(
            profile()
                .accept_multiplexer(TcpRelayStream::new(WriteGate {
                    inner: server,
                    blocked: server_blocked,
                }))
                .await
                .unwrap(),
        )
        .await;
    });
    let connection = ClientConnection::tcp(TcpRelayStream::new(client), "u", "p")
        .await
        .unwrap();
    (connection, blocked, server)
}

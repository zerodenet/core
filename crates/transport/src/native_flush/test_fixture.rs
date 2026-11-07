//! Actual production native options/control, with one explicit pending-flush seam.
use super::*;
use std::{future::Future, num::NonZeroUsize, pin::Pin};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{
    contract::{component::ResourceDeclaration, lifecycle::ChannelControl, CancellationToken},
    ControlledTcpListenerOwner, EgressInterfaceControl, PendingTcpConnect,
};
#[derive(Default)]
pub(crate) struct Gate {
    armed: AtomicBool,
    blocked: AtomicBool,
    wake: AtomicWaker,
    changed: tokio::sync::Notify,
}
impl Gate {
    pub(crate) fn hold(&self) {
        self.blocked.store(false, Ordering::Release);
        self.armed.store(true, Ordering::Release);
    }
    pub(crate) fn release(&self) {
        self.armed.store(false, Ordering::Release);
        self.wake.wake();
    }
    pub(crate) async fn blocked(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.blocked.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}
pub(crate) struct Held<S> {
    pub(crate) io: S,
    pub(crate) gate: Arc<Gate>,
}
impl<S: AsyncRead + Unpin> AsyncRead for Held<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, out)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Held<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.gate.wake.register(cx.waker());
        if this.gate.armed.load(Ordering::Acquire) {
            this.gate.blocked.store(true, Ordering::Release);
            this.gate.changed.notify_waiters();
            return Poll::Pending;
        }
        Pin::new(&mut this.io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
pub(crate) async fn pair() -> (
    Held<zero_platform_tokio::ControlledTcpStream>,
    zero_platform_tokio::AcceptedTcpStream,
    impl Future<Output = ()>,
) {
    let end = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
    let one = NonZeroUsize::new(1).unwrap();
    let declaration = ResourceDeclaration::TcpListener {
        destination: zero_core::request::Destination::new(
            zero_core::Address::Ipv4([127, 0, 0, 1]),
            0,
        ),
        accepted: None,
        max_bind_attempts: one,
    };
    let mut owner = ControlledTcpListenerOwner::new_with_accepted_limit(
        &declaration,
        one,
        CancellationToken::new(),
        one,
    )
    .unwrap();
    let mut listener = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr().unwrap();
    let selection = EgressInterfaceControl::default()
        .select_for_peer_with_policy(address, &Default::default())
        .unwrap();
    let (mut pending, mut native) = PendingTcpConnect::prepare(
        address,
        &Default::default(),
        &selection,
        CancellationToken::new(),
        None,
    )
    .unwrap();
    let (client, accepted) = tokio::join!(pending.connect(end), listener.accept_owned());
    let client = client.unwrap();
    let (accepted, _) = accepted.unwrap();
    let claim = accepted.claim();
    let (server, _) = accepted.into_parts();
    assert!(client.nodelay().unwrap() && server.nodelay().unwrap());
    let cleanup = async move {
        let native = native.wait_closed(end).await.unwrap();
        assert!(native.is_complete());
        assert_eq!(native.closed_io.len(), 1);
        let receipt = owner
            .close_accepted(&claim, end)
            .await
            .unwrap()
            .expect("same original accepted native");
        assert!(receipt.matches(&claim));
        drop(listener);
        let report = owner.wait_closed(end).await.unwrap();
        assert_eq!(owner.confirmed_close_count(), 1);
        assert!(!owner.native_unconfirmed());
        // Library accepted users are deliberately unaccounted; no Scope join is invented.
        assert!(!report.is_complete());
    };
    (
        Held {
            io: client,
            gate: Arc::default(),
        },
        server,
        cleanup,
    )
}

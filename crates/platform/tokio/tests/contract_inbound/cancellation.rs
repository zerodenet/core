use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

struct PendingRead {
    io: DuplexStream,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AsyncRead for PendingRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.io).poll_read(cx, bytes);
        if result.is_pending() {
            if let Some(pending) = self.pending.take() {
                let _ = pending.send(());
            }
        }
        result
    }
}
struct Waiting(PendingRead);
impl InboundHandshake for Waiting {
    async fn complete(mut self, _: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        // Deliberately ignore the token here: the public erasure seam must
        // enforce the bound rather than require another per-protocol wrapper.
        let _ = self.0.read_u8().await;
        panic!("fixture must remain suspended in the original read");
    }
}
#[tokio::test]
async fn pending_handshake_cancellation_retires_its_actual_owned_view_without_minting_identity() {
    let (io, mut peer) = tokio::io::duplex(64);
    let (pending, suspended) = tokio::sync::oneshot::channel();
    let cancel = CancellationToken::new();
    let completing = tokio::spawn(
        PendingInbound::new(Waiting(PendingRead {
            io,
            pending: Some(pending),
        }))
        .complete(HandshakeContext {
            accepted: None,
            connection: None,
            inbound: support::inbound(),
            identities: Arc::new(NoBusinessIdentity),
            deadline: Instant::now() + Duration::from_secs(60),
            cancel: cancel.clone(),
        }),
    );
    suspended.await.unwrap();
    assert!(
        !completing.is_finished(),
        "the actual read is still pending before cancellation"
    );
    cancel.cancel();
    let error = match completing.await.unwrap() {
        Err(error) => error,
        Ok(_) => panic!("cancelled request cannot be handed off"),
    };
    assert_eq!(
        error.reason,
        zero_platform_tokio::contract::Reason::Cancelled
    );
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
}
struct MustNotStart(Arc<std::sync::atomic::AtomicBool>);
impl InboundHandshake for MustNotStart {
    fn complete(
        self,
        _: HandshakeContext,
    ) -> impl std::future::Future<Output = Result<AcceptedRequest, ChannelError>> + Send {
        self.0.store(true, Ordering::SeqCst);
        std::future::pending()
    }
}
#[tokio::test]
async fn expired_handshake_budget_rejects_before_invoking_the_protocol() {
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let result = PendingInbound::new(MustNotStart(called.clone()))
        .complete(HandshakeContext {
            accepted: None,
            connection: None,
            inbound: support::inbound(),
            identities: Arc::new(NoBusinessIdentity),
            deadline: Instant::now() - Duration::from_secs(1),
            cancel: CancellationToken::new(),
        })
        .await;
    assert!(
        matches!(result,Err(error) if error.reason == zero_platform_tokio::contract::Reason::Timeout)
    );
    assert!(!called.load(Ordering::SeqCst));
}

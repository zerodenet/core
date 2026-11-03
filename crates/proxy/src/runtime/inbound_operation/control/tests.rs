use super::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{lifecycle::ChannelControl, CancellationToken};
use zero_platform_tokio::{ControlledTcpStream, TcpRelayStream, TokioSocket};
#[derive(Debug, Default)]
struct Reads(AtomicUsize);
impl zero_traits::IoObserver for Reads {
    fn stream_boundary(&self) {}
    fn received(&self, bytes: usize) {
        self.0.fetch_add(bytes, Ordering::SeqCst);
    }
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
async fn pair() -> (TokioSocket, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (socket, peer) = tokio::join!(
        TokioSocket::connect_addr(listener.local_addr().unwrap()),
        listener.accept()
    );
    (socket.unwrap(), peer.unwrap().0)
}
#[tokio::test]
async fn target_control_cancels_actual_rate_wait_and_returns_original_view() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (client, client_peer) = pair().await;
        let (target, mut target_peer) = pair().await;
        let reads = Arc::new(Reads::default());
        let target = target.with_observer(Some(reads.clone()));
        let (target, mut original) =
            ControlledTcpStream::new(target, CancellationToken::new()).unwrap();
        let control = zero_transport::handshake_target::forward_session(
            TcpRelayStream::from(client),
            TcpRelayStream::new(target),
            Vec::new(),
            Default::default(),
            zero_transport::handshake_target::RateLimit {
                after_bytes: 0,
                bytes_per_sec: 1,
                burst_bytes: 1,
            },
        );
        let proxy = crate::runtime::Proxy::new(
            zero_config::RuntimeConfig::parse(
                r#"{"inbounds":[],"route":{"final":{"type":"direct"}}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        let runtime = crate::runtime::route_runtime::InboundRouteRuntime::new(
            crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                proxy.tcp_runtime_services(),
            ),
            "website".into(),
            None,
        );
        let cancel = CancellationToken::new();
        let mut operation = Box::pin(run(runtime, control, Some(cancel.clone())));
        target_peer.write_all(&[0x41; 8192]).await.unwrap();
        std::future::poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            if reads.0.load(Ordering::SeqCst) > 1 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        // Actual TCP bytes were consumed, no exempt budget remains, and the
        // awaited forwarding future is Pending before any client write.
        let mut byte = [0];
        assert_eq!(
            client_peer.try_read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        cancel.cancel();
        operation.await.unwrap();
        let receipt = original
            .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert!(receipt.is_complete());
        assert_eq!(receipt.closed_io.len(), 1);
        assert_eq!(target_peer.read(&mut byte).await.unwrap(), 0);
    })
    .await
    .unwrap();
}

//! Real accepted native/Scope ownership cannot finish a queued response tail.
use super::*;
use futures_util::task::AtomicWaker;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        scope::{TaskClass, TaskScope},
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    ControlledTcpListenerOwner,
};
#[derive(Default)]
struct FlushGate {
    armed: AtomicBool,
    release: AtomicBool,
    wake: AtomicWaker,
    blocked: AtomicBool,
    changed: tokio::sync::Notify,
}
struct HeldFlush<S> {
    io: S,
    gate: Arc<FlushGate>,
}
impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for HeldFlush<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, out)
    }
}
impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for HeldFlush<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, bytes)
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.gate.armed.load(Ordering::Acquire) {
            this.gate.wake.register(cx.waker());
            if !this.gate.release.load(Ordering::Acquire) {
                this.gate.blocked.store(true, Ordering::Release);
                this.gate.changed.notify_one();
                return Poll::Pending;
            }
        }
        Pin::new(&mut this.io).poll_flush(cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
#[tokio::test]
async fn accepted_scoped_h2_final_data_and_end_require_real_original_flush_before_caller_and_driver_join(
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        let limit = NonZeroUsize::new(2).unwrap();
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(limit));
        let declaration = ResourceDeclaration::TcpListener {
            destination: zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                0,
            ),
            accepted: None,
            max_bind_attempts: one,
        };
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            one,
            CancellationToken::new(),
            one,
            limit,
            &scope.registrar(),
        )
        .unwrap();
        let mut listener = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let receiver = original.accepted_task_receiver().unwrap();
        scope.enable_registration().unwrap();
        let address = listener.local_addr().unwrap();
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), listener.accept_owned());
        let peer = peer.unwrap();
        let (accepted, _) = accepted.unwrap();
        let claim = accepted.claim();
        let gate = Arc::new(FlushGate::default());
        let actual = gate.clone();
        let (pending, waiting) = oneshot::channel();
        let (held, observed) = oneshot::channel();
        let (_, caller) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                move |socket, tasks| async move {
                    let mut io = zero_transport::h2::accept_h2_scoped(
                        HeldFlush {
                            io: socket,
                            gate: actual.clone(),
                        },
                        &profile(),
                        &tasks.caller()?,
                    )
                    .await
                    .map_err(failure)?;
                    let mut bytes = Vec::new();
                    io.read_to_end(&mut bytes).await.map_err(failure)?;
                    assert_eq!(bytes, vec![0x53; 256 * 1024]);
                    actual.armed.store(true, Ordering::Release);
                    io.write_all(&vec![0x79; 1025]).await.map_err(failure)?;
                    let mut closing = Box::pin(io.shutdown());
                    std::future::poll_fn(|cx| {
                        assert!(
                            closing.as_mut().poll(cx).is_pending(),
                            "END queued is not native flush"
                        );
                        Poll::Ready(())
                    })
                    .await;
                    pending.send(()).unwrap();
                    closing.await.map_err(failure)?;
                    held.send(()).unwrap();
                    Ok(())
                },
            )
            .unwrap();
        let client = tokio::spawn(async move {
            let mut io = connect_h2(peer, &profile(), "127.0.0.1", address.port())
                .await
                .unwrap();
            io.write_all(&vec![0x53; 256 * 1024]).await.unwrap();
            io.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            io.read_to_end(&mut bytes).await.unwrap();
            let expected = vec![0x79; 1025];
            assert_eq!(bytes, expected);
        });
        waiting.await.unwrap();
        while !gate.blocked.load(Ordering::Acquire) {
            gate.changed.notified().await;
        }
        assert!(!caller.is_joined());
        assert_eq!(original.accepted_evidence().unwrap().retained, 1);
        assert!(original.take_accepted_report().closed_io.is_empty());
        gate.release.store(true, Ordering::Release);
        gate.wake.wake();
        observed.await.unwrap();
        for _ in 0..2 {
            let actual = scope.next_exit().await.unwrap();
            assert!(actual.result.is_ok());
            let accepted = receiver.observe_joined(actual.id).unwrap();
            assert_eq!(accepted.id(), actual.id);
            assert!(accepted.result().is_ok() && accepted.registration_released());
        }
        let joined = caller.try_joined().unwrap();
        assert!(caller.matches_joined(&joined) && joined.registration_released());
        client.await.unwrap();
        let end = tokio::time::Instant::now() + Duration::from_secs(3);
        assert!(original
            .close_accepted_graph(&claim, end)
            .await
            .unwrap()
            .unwrap()
            .matches(&claim));
        assert!(original.wait_closed(end).await.unwrap().is_complete());
        assert!(scope.shutdown(end).await.is_complete());
    })
    .await
    .unwrap();
}

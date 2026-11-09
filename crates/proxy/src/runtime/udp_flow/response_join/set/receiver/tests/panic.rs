use super::*;
struct PanicsWithOriginalBox {
    socket: UdpSocket,
    dropped: Arc<AtomicBool>,
}
impl Future for PanicsWithOriginalBox {
    type Output = Result<(), ChannelError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut bytes = [0; 16];
        let mut buffer = tokio::io::ReadBuf::new(&mut bytes);
        if self.socket.poll_recv_from(cx, &mut buffer).is_ready() {
            panic!("actual original packet RX poll panic");
        }
        Poll::Pending
    }
}
impl Drop for PanicsWithOriginalBox {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
#[tokio::test]
async fn actual_rx_poll_panic_keeps_the_original_box_until_its_real_critical_task_join_then_cleanup(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (_proxy, memory) = memory();
        let sources = sources(&memory);
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let original = Arc::new(std::sync::OnceLock::new());
        let actual = original.clone();
        let (begin, begun) = oneshot::channel();
        let (ready, receiving) = oneshot::channel();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let destroyed = dropped.clone();
        let worker_memory = memory.clone();
        let (id, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                begun.await.unwrap();
                let (owner, mut lease) = ResponseSetOwner::new(JoinSet::new());
                lease
                    .install_packet_receiver(
                        &worker_memory,
                        actual.get().unwrap().clone(),
                        CancellationToken::new(),
                        sources.into_error(),
                        PanicsWithOriginalBox {
                            socket,
                            dropped: destroyed,
                        },
                    )
                    .unwrap();
                poll_fn(|cx| {
                    let mut next = std::pin::pin!(lease.join_next_with_id());
                    assert!(next.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                ready
                    .send(owner)
                    .unwrap_or_else(|_| panic!("original owner"));
                let _ = lease.join_next_with_id().await;
                unreachable!("actual original Critical poll panics");
            })
            .unwrap();
        original.set(witness.clone()).unwrap();
        begin.send(()).unwrap();
        let owner = receiving.await.unwrap();
        peer.send_to(b"panic", address).await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), id);
        let source = receipt.result().unwrap_err();
        assert!(Arc::ptr_eq(
            &source,
            &witness.try_joined().unwrap().result().unwrap_err()
        ));
        assert!(
            !dropped.load(Ordering::Acquire),
            "unwind restored the original Box"
        );
        assert!(!receipt.joined().registration_released());
        let closed = owner.shutdown(end()).await.unwrap();
        assert!(closed.matches(&owner));
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(closed.joined_tasks(), 0);
        assert!(
            closed.packet_failure().is_none(),
            "the real Scope receipt owns this panic, not a fabricated work error"
        );
        assert!(!receipt.joined().registration_released());
        witness
            .acknowledge_hosted_failure(receipt.joined())
            .unwrap();
        scope.acknowledge(&receipt).unwrap();
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

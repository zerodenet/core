//! Real original registrar + actual UDP response work, with held task/native gates.
use super::*;
use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
use zero_platform_tokio::contract::scope::TaskJoinWitness;

#[tokio::test]
async fn actual_udp_receiver_is_unique_and_retains_its_own_real_failed_exit_before_host_ack() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let proxy = Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let registry = Arc::new(InboundOwnerRegistry::default());
        let (stop, _) = watch::channel(false);
        let installed = registry
            .install("actual-response-caller", &proxy.protocols, 1, stop)
            .unwrap();
        let group = installed
            .child_tasks(NonZeroUsize::new(1).unwrap())
            .unwrap();
        let host = installed.tasks();
        let owner = installed.owner();
        let native = NativeObligations::new(1, 1, 1);
        installed.register(native.resource()).unwrap();
        let (parent, parent_done) = oneshot::channel();
        owner
            .spawn_driver(async move {
                parent_done.await.map_err(std::io::Error::other)?;
                Ok(())
            })
            .unwrap();
        let (primary, primary_done) = oneshot::channel();
        let (bound, binding) = oneshot::channel();
        group
            .spawn_bound(&host, move |binding| async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                bound
                    .send(binding)
                    .unwrap_or_else(|_| panic!("actual source binding receiver"));
                primary_done.await.map_err(std::io::Error::other)?;
                Ok(())
            })
            .unwrap();
        let binding = binding.await.unwrap();
        let caller = binding.join_witness().unwrap();
        native.bind_source(caller.scope_identity()).unwrap();
        let mut permit = native.reserve(ChildKind::Udp).unwrap();
        permit.bind_caller(binding).unwrap();
        permit.enter().unwrap();
        let registration = permit.registration();
        // This fixture's actual raw Tokio socket deliberately has no native
        // provider close receipt. The task test must not turn it into coverage.
        registration.declare_gateway(false).unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let expected_peer = peer.local_addr().unwrap();
        let (read_pending, pending) = oneshot::channel();
        let (panic_now, panicking) = oneshot::channel();
        let (_, receiver) = registration
            .spawn_datagram_receiver(move |actual: TaskJoinWitness| async move {
                assert!(!actual.is_joined());
                let mut bytes = [0; 16];
                let receive = socket.recv_from(&mut bytes);
                tokio::pin!(receive);
                std::future::poll_fn(|cx| {
                    assert!(receive.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                read_pending.send(()).unwrap();
                let (read, remote) = receive.await.unwrap();
                assert_eq!(read, 4);
                assert_eq!(remote, expected_peer);
                panicking.await.unwrap();
                panic!("actual original packet response worker panic")
            })
            .unwrap();
        pending.await.unwrap();
        assert!(registration
            .spawn_datagram_receiver(|_| async { Ok(()) })
            .is_err());
        assert!(!receiver.same_registration(&caller));
        assert!(receiver
            .scope_identity()
            .same_scope(&caller.scope_identity()));
        assert!(!registration.receiver_released().unwrap());
        assert_eq!(peer.send_to(b"data", address).await.unwrap(), 4);
        panic_now.send(()).unwrap();
        primary.send(()).unwrap();
        parent.send(()).unwrap();
        drop(permit);
        assert!(registry
            .confirm_shutdown(
                "actual-response-caller",
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .is_err());
        let joined = receiver
            .try_joined()
            .expect("real Source consumes original receiver exit");
        assert!(joined.result().is_err());
        registration.receiver_released().unwrap();
        let received = native
            .0
            .lock()
            .unwrap()
            .records
            .get(&registration.id)
            .unwrap()
            .receiver
            .as_ref()
            .unwrap()
            .joined
            .clone()
            .unwrap();
        assert!(receiver.matches_joined(&received));
        assert!(Arc::ptr_eq(
            &joined.result().unwrap_err(),
            &received.result().unwrap_err()
        ));
        assert!(received.failure_accepted());
        // Real UDP view Drop and both actual task joins still do not create
        // the missing provider/native closure for this entered gateway.
        assert_eq!(native.0.lock().unwrap().records.len(), 1);
        assert!(owner.terminal_for_test().is_none());
    })
    .await
    .unwrap();
}

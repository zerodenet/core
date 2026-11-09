//! Exact Root reservation precedes real setup registration and first UDP I/O.
use super::*;
use crate::contract::network::UpstreamOpener;

#[tokio::test]
async fn original_setup_reserves_before_actor_and_keeps_real_udp_until_original_actor_ack() {
    timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let totals = CarrierOriginalTotals::new(one);
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = destination(remote);
        let limits = MetadataLimits {
            main_sockets: one,
            probe_sockets: one,
            // Existing native GSO/GRO initialization may test v4 and v6.
            // This positive original/ACK fixture does not test probe quota.
            probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
            concurrent_send_calls: one,
            write_pollers: one,
        };
        let metadata = MetadataPoolOwner::new(instance(), one, limits, &host).unwrap();
        metadata
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataCarrier {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    limits,
                    max_origins: one,
                    ports: vec![],
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        metadata.start().unwrap();

        // No actor, initializer future, native acquisition or waiter exists yet.
        let cold = metadata.grants().prepare_original(&tasks).unwrap();
        let unstarted = cold.origin();
        assert!(scope.unjoined_tasks().is_empty());
        assert_eq!(totals.snapshot().charged, 1);
        rejected::check(&metadata, &tasks);
        assert!(scope.unjoined_tasks().is_empty());
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(
            metadata
                .grants()
                .prepare_original(&tasks)
                .err()
                .unwrap()
                .reason,
            Reason::Capacity
        );
        drop(cold);
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let no_native = unstarted.closed().unwrap();
        assert!(no_native.matches(&unstarted) && !no_native.had_native_acquisition());

        let cold = metadata.grants().prepare_original(&tasks).unwrap();
        let origin = cold.origin();
        let (begin, started) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (permit, caller) = cold
            .spawn_auxiliary(&tasks, async move {
                if started.await.is_ok() {
                    let _ = finished.await;
                }
                Ok(())
            })
            .unwrap();
        begin.send(()).unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None).unwrap();
        let candidates = opener
            .resolve_controlled_carrier(&binding, endpoint, context())
            .await
            .unwrap();
        let (_, mut grant) = candidates
            .open_metadata(0, None, &permit, &caller, deadline())
            .unwrap();
        let message = MetadataTransmit {
            destination: remote,
            contents: b"gated actual original",
            source_ip: None,
            ecn: None,
            segment_size: None,
        };
        let mut writable = grant.tx.create_write_poller().unwrap();
        std::future::poll_fn(|cx| loop {
            std::task::ready!(writable.poll_writable(cx)).unwrap();
            match grant.tx.try_send(&message) {
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                result => return Poll::Ready(result.unwrap()),
            }
        })
        .await;
        let mut bytes = [0; 64];
        let (size, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], message.contents);
        drop(writable);
        let mut claim = grant.users_claim.take();
        drop(grant);
        permit.return_unbound(&mut claim, &tasks).unwrap();
        let closed = permit.close(None, &tasks).unwrap();
        assert!(closed.matches(&origin) && closed.had_native_acquisition());
        assert_eq!(totals.snapshot().charged, 1);
        finish.send(()).unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(caller.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert_eq!(totals.snapshot().charged, 1);
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&receipt).unwrap();
        assert_eq!(totals.snapshot().charged, 1);
        drop(deferred);
        caller.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(metadata.shutdown().unwrap().is_complete());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

mod phase;

#[path = "preparation/rejected.rs"]
mod rejected;

#[path = "preparation/protocol_observer.rs"]
mod protocol_observer;

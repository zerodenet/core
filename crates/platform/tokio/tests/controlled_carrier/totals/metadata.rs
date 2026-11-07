use super::*;
use crate::controlled_datagram::{
    metadata::{MetadataLimits, MetadataTransmit},
    metadata_pool::MetadataPoolOwner,
};
#[tokio::test]
async fn actual_metadata_query_main_probes_and_carrier_share_one_custody_total_and_original_query_ack(
) {
    timeout(Duration::from_secs(3), async {
        let n = NonZeroUsize::new(1).unwrap();
        let totals = CarrierOriginalTotals::new(n);
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(n));
        scope.enable_registration().unwrap();
        let host = Arc::new(CancellationToken::new());
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let limits = MetadataLimits {
            main_sockets: n,
            probe_sockets: n,
            probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
            concurrent_send_calls: n,
            write_pollers: n,
        };
        let metadata = MetadataPoolOwner::new(instance(), n, limits, &host).unwrap();
        let registered = metadata.registration();
        registered
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataDatagram {
                    endpoint: peer.local_addr().unwrap(),
                    dial: Default::default(),
                    limits,
                    max_origins: n,
                },
                &scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        metadata.start().unwrap();
        let mut carrier = Fixture::cold(1).await;
        carrier
            .owner
            .registration()
            .bind_declared_with_totals(
                &description(&carrier, 1),
                &scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        carrier.owner.start(deadline()).await.unwrap();
        let grants = metadata.grants();
        let (begin, started) = tokio::sync::oneshot::channel();
        let (publish, published) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let remote = peer.local_addr().unwrap();
        let (_, query) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let actual = started.await.unwrap();
                let permit = grants.reserve(&actual).unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let grant = permit
                    .open(
                        &actual,
                        &egress,
                        NetworkGeneration(egress.generation()),
                        deadline(),
                    )
                    .unwrap();
                let message = MetadataTransmit {
                    destination: remote,
                    contents: b"actual query",
                    source_ip: None,
                    ecn: None,
                    segment_size: None,
                };
                let mut writable = grant.tx.create_write_poller().unwrap();
                let sent = std::future::poll_fn(|cx| loop {
                    std::task::ready!(writable.poll_writable(cx)).unwrap();
                    match grant.tx.try_send(&message) {
                        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
                            continue;
                        }
                        actual => return Poll::Ready(actual.unwrap()),
                    }
                })
                .await;
                assert!(sent.complete);
                assert_eq!((sent.bytes, sent.datagrams), (message.contents.len(), 1));
                drop(writable);
                assert!(publish.send((permit, grant)).is_ok());
                finished.await.unwrap();
                Ok(())
            })
            .unwrap();
        begin.send(query.clone()).unwrap();
        let mut wire = [0; 64];
        let (read, _) = peer.recv_from(&mut wire).await.unwrap();
        assert_eq!(&wire[..read], b"actual query");
        let (permit, mut grant) = published.await.unwrap();
        let origin = permit.origin();
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(
            carrier.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        let mut claim = grant.users_claim.take();
        drop(grant);
        permit
            .return_unbound(&mut claim, &scope.registrar())
            .unwrap();
        let native = permit.close(None, &scope.registrar()).unwrap();
        assert!(native.matches(&origin) && native.had_native_acquisition());
        assert!(!query.is_joined());
        assert_eq!(
            totals.snapshot().charged,
            1,
            "native/users returned but actual caller still pending"
        );
        drop(metadata); // Root strongly retains this exact original and its receipt.
        assert_eq!(totals.snapshot().charged, 1);
        finish.send(()).unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(receipt.result().is_ok() && query.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert_eq!(totals.snapshot().charged, 1);
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&receipt).unwrap();
        assert_eq!(totals.snapshot().charged, 1);
        drop(deferred);
        let actual = query.wait_registration_released().await;
        assert!(query.matches_joined(&actual) && actual.registration_released());
        assert_eq!(totals.snapshot().charged, 0);
        drop(carrier.grants.reserve().unwrap());
        assert_eq!(totals.snapshot().charged, 0);
        assert!(carrier
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

mod local_credit;
mod preparation;

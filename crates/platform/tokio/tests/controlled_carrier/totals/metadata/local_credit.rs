//! Root total exceeds local N: only actual ACK can refund that same owner's N.
use super::*;
use crate::{
    contract::{
        component::GrantedMetadataListener,
        scope::{TaskJoinWitness, TaskRegistrar},
    },
    controlled_datagram::metadata_pool::MetadataPoolPermit,
};
use tokio::sync::oneshot;
async fn submit(
    started: oneshot::Receiver<(MetadataPoolPermit, TaskJoinWitness)>,
    returned: oneshot::Sender<(MetadataPoolPermit, GrantedMetadataListener)>,
    remote: std::net::SocketAddr,
) -> Result<(), ChannelError> {
    let (permit, caller) = started.await.unwrap();
    let egress = crate::EgressInterfaceControl::default();
    let grant = permit
        .open(
            &caller,
            &egress,
            NetworkGeneration(egress.generation()),
            deadline(),
        )
        .unwrap();
    let message = MetadataTransmit {
        destination: remote,
        contents: b"same original owner",
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
    drop(writable);
    assert!(returned.send((permit, grant)).is_ok());
    Ok(())
}
fn close_native(
    permit: &MetadataPoolPermit,
    mut grant: GrantedMetadataListener,
    tasks: &TaskRegistrar,
) {
    let original = permit.origin();
    let mut claim = grant.users_claim.take();
    drop(grant);
    permit.return_unbound(&mut claim, tasks).unwrap();
    let closed = permit.close(None, tasks).unwrap();
    assert!(closed.matches(&original) && closed.had_native_acquisition());
}
#[tokio::test]
async fn local_original_n1_stays_full_after_real_udp_native_close_until_its_actual_host_ack() {
    timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let two = NonZeroUsize::new(2).unwrap();
        let totals = CarrierOriginalTotals::new(two);
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(two));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let limits = MetadataLimits {
            main_sockets: one,
            probe_sockets: one,
            probe_attempts_per_initialization: one,
            concurrent_send_calls: one,
            write_pollers: one,
        };
        let metadata = MetadataPoolOwner::new(instance(), one, limits, &host).unwrap();
        let registration = metadata.registration();
        registration
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledMetadataDatagram {
                    endpoint: remote,
                    dial: Default::default(),
                    limits,
                    max_origins: one,
                },
                &tasks,
                &host,
                &totals,
            )
            .unwrap();
        metadata.start().unwrap();
        let grants = metadata.grants();
        let (begin1, started1) = oneshot::channel();
        let (publish1, published1) = oneshot::channel();
        let (_, first) = tasks
            .spawn_tracked(TaskClass::Auxiliary, submit(started1, publish1, remote))
            .unwrap();
        let permit1 = grants.reserve(&first).unwrap();
        let original1 = permit1.origin();
        assert!(begin1.send((permit1, first.clone())).is_ok());
        let mut bytes = [0; 64];
        let (size, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"same original owner");
        let (permit1, grant1) = published1.await.unwrap();
        close_native(&permit1, grant1, &tasks);
        let receipt1 = scope.next_receipt().await.unwrap();
        assert!(first.matches_joined(receipt1.joined()) && receipt1.result().is_ok());
        assert!(!receipt1.joined().registration_released());
        assert_eq!(grants.retained(), Some(1));
        assert_eq!(totals.snapshot().charged, 1);
        assert!(original1.closed().unwrap().had_native_acquisition());

        // A different real pending setup uses the same declared Scope credit.
        // The larger Root has room; this refusal must be local N1, not StalePlan
        // from trying to reuse the already-joined first task as another caller.
        let (begin2, started2) = oneshot::channel();
        let (publish2, published2) = oneshot::channel();
        let (_, second) = tasks
            .spawn_tracked(TaskClass::Auxiliary, submit(started2, publish2, remote))
            .unwrap();
        let refused = grants.reserve(&second).err().unwrap();
        assert_eq!(refused.reason, Reason::Capacity);
        assert_eq!(refused.delivery, DeliveryState::NotSubmitted);
        assert_eq!(totals.snapshot().charged, 1);
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&receipt1).unwrap();
        assert!(!receipt1.joined().registration_released());
        assert_eq!(
            grants.reserve(&second).err().unwrap().reason,
            Reason::Capacity
        );
        assert_eq!(grants.retained(), Some(1));
        drop(deferred);
        first.wait_registration_released().await;
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if totals.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(grants.retained(), Some(0));
        let permit2 = grants.reserve(&second).unwrap();
        let original2 = permit2.origin();
        assert!(!original2.same_original(&original1));
        assert!(original2.owner().same_owner(original1.owner()));
        assert!(begin2.send((permit2, second.clone())).is_ok());
        let (size, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"same original owner");
        let (permit2, grant2) = published2.await.unwrap();
        close_native(&permit2, grant2, &tasks);
        let receipt2 = scope.next_receipt().await.unwrap();
        assert!(second.matches_joined(receipt2.joined()) && receipt2.result().is_ok());
        assert_eq!(totals.snapshot().charged, 1);
        assert_eq!(grants.retained(), Some(1));
        scope.acknowledge(&receipt2).unwrap();
        second.wait_registration_released().await;
        assert_eq!(totals.snapshot().charged, 0);
        assert_eq!(grants.retained(), Some(0));
        assert!(metadata.shutdown().unwrap().is_complete());
        assert!(registration.closed().unwrap().matches(&registration));
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

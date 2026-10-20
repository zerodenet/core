use super::*;
use zero_platform_tokio::contract::{
    lifecycle::TaskClass,
    scope::{HostedScope, SpawnError},
};

fn hosted() -> HostedScope {
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        Some(NonZeroUsize::new(1).unwrap()),
    );
    scope.enable_registration().unwrap();
    scope
}

#[tokio::test]
async fn hosted_original_h2_failure_waits_for_persistent_local_acceptance_and_native_close_separately(
) {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, mut peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = hosted();
        let pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(4, CancellationToken::new()),
        )
        .unwrap();
        // The actual caller owner retains this metadata alongside its original
        // CarrierOwner even after all request-facing handles are dropped.
        let receiver = pending.local_task_receiver();
        let witness = pending.join_witness().clone();
        drop(pending);
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.class(), TaskClass::Auxiliary);
        let original = receipt.result().unwrap_err();
        assert!(receiver.received().is_none());
        let joined = witness.wait().await;
        assert!(Arc::ptr_eq(&original, &joined.result().unwrap_err()));
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        assert!(adoption.closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        let received = receiver.receive_joined().unwrap().unwrap();
        assert_eq!(received.id(), receipt.id());
        assert_eq!(received.class(), receipt.class());
        assert!(Arc::ptr_eq(&original, &received.result().unwrap_err()));
        assert!(Arc::ptr_eq(
            &original,
            &receiver.received().unwrap().result().unwrap_err()
        ));
        // A real owner acceptance and the supervisor's publication ACK are
        // distinct. Both refer to this one original scope entry.
        assert!(receipt.local_failure_accepted());
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        scope.acknowledge(&receipt).unwrap();
        receiver.receive_joined().unwrap(); // same receiver/receipt is retry-safe
        assert!(adoption.closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        let healthy = scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        let success = scope.next_receipt().await.unwrap();
        assert_eq!(success.id(), healthy);
        assert!(success.result().is_ok());
        scope.acknowledge(&success).unwrap();
        let report = adoption.retire(end()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(fixture.owner.retained(), 0);
        assert!(adoption.closed().unwrap().matches(&adoption));
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_all_h2_receivers_never_accepts_a_real_joined_auxiliary_failure() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, mut peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = hosted();
        let pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(4, CancellationToken::new()),
        )
        .unwrap();
        drop(pending);
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        let receipt = scope.next_receipt().await.unwrap();
        assert!(receipt.result().is_err());
        assert_eq!(receipt.class(), TaskClass::Auxiliary);
        scope.begin_shutdown();
        assert!(scope.is_joined());
        assert!(
            !scope.is_drained(),
            "no local receiver accepted the original failure"
        );
        assert!(adoption.retire(end()).await.unwrap().is_complete());
        assert_eq!(fixture.owner.retained(), 0);
        assert!(!scope.is_drained(), "native close is not failure delivery");
        // No manufactured acceptance is used to drain this diagnostic fixture.
        // All actual tasks and native I/O are already joined/closed; the genuine
        // unpublished error remains in this same scope until its owner drops.
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hosted_pending_h2_error_keeps_first_connection_arc_and_receives_the_distinct_aggregate_exit(
) {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, mut peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = hosted();
        let cancel = CancellationToken::new();
        let mut pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(4, cancel.clone()),
        )
        .unwrap();
        let receiver = pending.local_task_receiver();
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        cancel.cancel();
        let receipt = scope.next_receipt().await.unwrap();
        let aggregate = receipt.result().unwrap_err();
        let first = pending.failure.get().unwrap();
        assert!(
            !Arc::ptr_eq(&first, &aggregate),
            "the forwarded aggregate error is a distinct original object"
        );
        let observed = pending.ready().await.err().unwrap();
        assert!(Arc::ptr_eq(&first, &observed));
        let received = receiver.received().unwrap();
        assert!(Arc::ptr_eq(&aggregate, &received.result().unwrap_err()));
        assert_eq!(observed.stage, aggregate.stage);
        assert_eq!(observed.reason, aggregate.reason);
        assert_eq!(observed.impact, aggregate.impact);
        assert_eq!(observed.component, aggregate.component);
        assert_eq!(observed.session, aggregate.session);
        assert_eq!(observed.delivery, aggregate.delivery);
        assert_eq!(observed.retry, aggregate.retry);
        scope.acknowledge(&receipt).unwrap();
        assert!(adoption.closed().is_none());
        drop(pending);
        assert!(adoption.retire(end()).await.unwrap().is_complete());
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

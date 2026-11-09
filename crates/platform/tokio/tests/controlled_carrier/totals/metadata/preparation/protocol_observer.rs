//! An actual finite receiver holds the exact pre-IO UDP original through ACK.
use super::*;
use crate::contract::{
    lifecycle::{CloseReport, DynChannelControl},
    network::{PendingCarrierObserver, UpstreamTransport},
    scope::TaskScopeIdentity,
    BoxFuture, CloseReason,
};
use crate::controlled_carrier::ProtocolOriginal;
use crate::controlled_datagram::{
    metadata::MetadataViewClaim,
    metadata_pool::{MetadataPoolOrigin, MetadataPoolPermit},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, OnceLock,
};
#[path = "protocol_observer/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "protocol_observer/refusal.rs"]
mod refusal;
#[tokio::test]
async fn exact_protocol_observer_is_strong_before_udp_and_settles_only_after_actual_native_and_host_ack(
) {
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
            probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
            concurrent_send_calls: one,
            write_pollers: one,
        };
        let owner = MetadataPoolOwner::new(instance(), one, limits, &host).unwrap();
        owner
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
        owner.start().unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let (receiver, upstream) = observer(&binding, &tasks);
        let (foreign, other) = observer(&binding, &tasks);
        let retained = Arc::new(OriginalClose {
            ready: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
            origin: OnceLock::new(),
            tasks: tasks.clone(),
            wire: Mutex::new(None),
            claim: Mutex::new(None),
        });
        let (prepared, retirement) = owner
            .grants()
            .prepare_protocol_original(&tasks, Box::new(Control(retained.clone())).into())
            .unwrap_or_else(|error| panic!("{:?}", error.error));
        let origin = prepared.origin();
        assert!(retained.origin.set(origin.clone()).is_ok());
        assert!(
            prepared
                .protocol_original(
                    &upstream,
                    other.protocol_original_observer().unwrap(),
                    &endpoint,
                    &retirement
                )
                .is_err(),
            "a same-valued foreign anchor cannot be sealed as this upstream's original"
        );
        let actual = prepared
            .protocol_original(
                &upstream,
                upstream.protocol_original_observer().unwrap(),
                &endpoint,
                &retirement,
            )
            .unwrap();
        assert!(
            actual.binding() == &binding
                && actual.scope_identity().same_scope(&tasks.scope_identity())
        );
        assert!(other
            .protocol_original_observer()
            .unwrap()
            .retain_protocol_original(actual.clone())
            .is_err());
        assert!(foreign.original.lock().unwrap().is_none());
        upstream
            .protocol_original_observer()
            .unwrap()
            .retain_protocol_original(actual.clone())
            .unwrap();
        assert!(receiver
            .original
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .same_original(&actual));
        assert!(scope.unjoined_tasks().is_empty());
        assert!(origin.endpoints().is_none());
        assert!(!actual.is_settled());
        assert_eq!(totals.snapshot().charged, 1);
        let (begin, started) = tokio::sync::oneshot::channel::<(
            Arc<MetadataPoolPermit>,
            crate::contract::scope::TaskJoinWitness,
        )>();
        let work = retained.clone();
        let actor_binding = binding.clone();
        let (permit, actor) = prepared
            .spawn_auxiliary(&tasks, async move {
                let (permit, actor) = started.await.map_err(|source| {
                    ChannelError::new(
                        Stage::Prepare,
                        Reason::Internal,
                        crate::contract::Impact::Component,
                    )
                    .with_source(source)
                })?;
                let opener = NativeCarrierOpener::new(
                    instance(),
                    actor_binding.clone(),
                    remote,
                    egress,
                    None,
                )?;
                let candidates = opener
                    .resolve_controlled_carrier(&actor_binding, endpoint, context())
                    .await?;
                let (_, grant) = candidates.open_metadata(0, None, &permit, &actor, deadline())?;
                *work.wire.lock().unwrap() = Some(grant);
                let (sender, mut writable) = {
                    let wire = work.wire.lock().unwrap();
                    let wire = wire.as_ref().unwrap();
                    (wire.tx.clone(), wire.tx.create_write_poller().unwrap())
                };
                let message = MetadataTransmit {
                    destination: remote,
                    contents: b"same pre-delivery protocol original",
                    source_ip: None,
                    ecn: None,
                    segment_size: None,
                };
                std::future::poll_fn(|cx| loop {
                    std::task::ready!(writable.poll_writable(cx)).unwrap();
                    match sender.try_send(&message) {
                        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => continue,
                        result => return Poll::Ready(result.unwrap()),
                    }
                })
                .await;
                Ok(())
            })
            .unwrap();
        let permit = Arc::new(permit);
        assert!(begin.send((permit.clone(), actor.clone())).is_ok());
        let mut bytes = [0; 64];
        let (size, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"same pre-delivery protocol original");
        let receipt = scope.next_receipt().await.unwrap();
        assert!(actor.matches_joined(receipt.joined()) && receipt.result().is_ok());
        assert!(!receipt.joined().registration_released());
        actual.request_retirement();
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            Poll::Ready(())
        })
        .await;
        assert!(!actual.is_settled() && origin.closed().is_none());
        assert_eq!(totals.snapshot().charged, 1);
        retained.ready.store(true, Ordering::Release);
        retained.changed.notify_waiters();
        std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if origin.closed().is_some() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let closed = origin.closed().unwrap();
        assert!(closed.matches(&origin) && closed.had_native_acquisition());
        assert!(!actual.is_settled());
        assert_eq!(totals.snapshot().charged, 1);
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&receipt).unwrap();
        assert!(!actual.is_settled());
        drop(deferred);
        assert!(actor.try_joined().unwrap().registration_released());
        // This explicit barrier stops before the Root's next scan. The local
        // owner has genuine native/registration ACK proof and can remove its
        // own slot, but the same Root row is still charged.
        permit.close(None, &tasks).unwrap();
        let observed = origin.registration_observation();
        assert!(observed.released().unwrap().matches(&observed));
        assert_eq!(owner.grants().retained(), Some(0));
        assert_eq!(totals.snapshot_without_reconcile().charged, 1);
        assert!(!observed.root_returned());
        assert!(!retirement.is_settled() && !actual.is_settled());
        let wake = Arc::new(CountWake(std::sync::atomic::AtomicUsize::new(0)));
        let waker = std::task::Waker::from(wake.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut waiting = Box::pin(actual.wait_settled_original());
        assert!(std::future::Future::poll(waiting.as_mut(), &mut cx).is_pending());
        let before = wake.0.load(Ordering::Acquire);
        // Only removing the exact global row publishes and wakes this already
        // registered waiter. No second wait future or timer advances it.
        totals.poll_cancelled_retirements(&mut cx);
        assert_eq!(totals.snapshot_without_reconcile().charged, 0);
        assert!(wake.0.load(Ordering::Acquire) > before);
        assert!(matches!(
            std::future::Future::poll(waiting.as_mut(), &mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(actual.is_settled() && retirement.is_settled());
        assert!(observed.root_returned());
        drop(waiting);
        drop(permit);
        refusal::known_unregistered_original_returns_while_same_scope_real_task_remains_pending(
            &mut scope,
            &tasks,
            &owner,
            &totals,
            &upstream,
            &other,
            &destination(remote),
        )
        .await;
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

struct CountWake(std::sync::atomic::AtomicUsize);
impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Release);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

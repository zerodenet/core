use super::*;
use std::{
    mem::size_of,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Weak,
    },
};

#[derive(Default)]
struct Observation {
    shared: Mutex<Weak<Shared>>,
    sending: Mutex<Option<DirectEndpointQueueUsage>>,
    discarded: AtomicUsize,
    changed: Notify,
}
impl std::fmt::Debug for Observation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActualQueueObservation")
            .finish_non_exhaustive()
    }
}
impl zero_traits::IoObserver for Observation {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {
        let same_owner = self.shared.lock().unwrap().upgrade().unwrap();
        // This callback follows the actual original native UDP send, while its
        // command descriptor/payload still belongs to the executing driver.
        *self.sending.lock().unwrap() = Some(same_owner.copies.snapshot());
    }
    fn error(&self) {}
    fn dropped(&self) {
        self.discarded.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_one();
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        assert_eq!(reason, zero_traits::PacketDropReason::QueueFull);
        self.dropped();
    }
}
#[tokio::test]
async fn actual_driver_queue_snapshots_keep_zero_length_entries_and_recycle_after_real_cancel_and_native_close(
) {
    let fixture = Fixture::new().await;
    let observation = Arc::new(Observation::default());
    let services = fixture
        .proxy
        .tcp_runtime_services()
        .with_io_observer(Some(observation.clone()));
    let mut grants = limits();
    grants.logical = NonZeroUsize::new(1).unwrap();
    grants.native_sockets = NonZeroUsize::new(1).unwrap();
    grants.pending_bytes = NonZeroUsize::new(128).unwrap();
    let mut owner = DirectEndpointOwner::new(
        fixture.identity,
        crate::protocol_registry::UdpRuntimeServices::new(services).network(),
        None,
        UdpReceiveQueueConfig {
            capacity: 1,
            max_payload_bytes: None,
            max_stack_payload_bytes: None,
        },
        grants,
        TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        ),
        Some(observation.clone()),
    )
    .unwrap();
    *observation.shared.lock().unwrap() = Arc::downgrade(&owner.shared);
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let initial = endpoint.queue_usage();
    assert_eq!(initial.owned.entries, 0);
    assert_eq!(initial.owned.bytes, 0);
    assert_eq!(
        initial.entry_limit, 3,
        "one original command queue + logical RX queue + executing command"
    );
    assert_eq!(initial.byte_limit, 128);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let domain = Address::Domain("copy.test".into());
    let mut old_receivers = Vec::new();
    for batch in 0..8 {
        let (mut tx, rx, mut control) = fixture.open(&endpoint, target, CancellationToken::new());
        let sent = tx.send_new_packet(&domain, &[target], b"").await.unwrap();
        assert_eq!(sent.sent, 0);
        let mut bytes = [0; 64];
        let (size, original) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(size, 0);
        let live = observation
            .sending
            .lock()
            .unwrap()
            .take()
            .expect("actual native send callback");
        assert_eq!(live.owned.entries, 1);
        assert_eq!(
            live.owned.bytes,
            size_of::<SocketAddr>() + "copy.test".len()
        );
        assert_eq!(live.owned, live.in_flight_send);
        assert_eq!(live.queued_send.entries, 0);
        assert_eq!(live.queued_receive.entries, 0);
        assert_eq!(
            endpoint.queue_usage().owned.entries,
            0,
            "actual command storage was released after send"
        );
        let logical = owner
            .shared
            .state
            .lock()
            .unwrap()
            .sessions
            .values()
            .next()
            .unwrap()
            .logical
            .clone();
        let payload = if batch == 0 {
            &b""[..]
        } else {
            &b"rxbytes"[..]
        };
        peer.send_to(payload, original).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = logical.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if endpoint.queue_usage().queued_receive.entries == 1 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
        let queued = endpoint.queue_usage();
        assert_eq!(queued.owned, queued.queued_receive);
        assert_eq!(queued.owned.entries, 1);
        assert_eq!(queued.owned.bytes, payload.len());
        assert!(tokio::net::UdpSocket::bind(original).await.is_err());
        peer.send_to(b"full queue must preserve the existing copy", original)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), observation.changed.notified())
            .await
            .unwrap();
        assert_eq!(
            endpoint.queue_usage(),
            queued,
            "queue pressure neither clears live copies nor resets history"
        );
        control.cancel(zero_platform_tokio::contract::CloseReason::Completed);
        assert_eq!(
            endpoint.queue_usage(),
            queued,
            "cancel request alone releases no queued storage"
        );
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        let closed = endpoint.queue_usage();
        assert_eq!(closed.owned.entries, 0);
        assert_eq!(closed.owned.bytes, 0);
        assert_eq!(closed.queued_receive.entries, 0);
        assert_eq!(closed.peak_entries, 1);
        assert_eq!(
            closed.peak_bytes,
            size_of::<SocketAddr>() + "copy.test".len()
        );
        tokio::net::UdpSocket::bind(original).await.unwrap();
        old_receivers.push(rx);
    }
    assert_eq!(observation.discarded.load(Ordering::Acquire), 8);
    for mut rx in old_receivers {
        assert!(rx.recv().await.unwrap().is_none());
    }
    let history = endpoint.queue_usage();
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 8 original Direct UDP sockets"]
    );
    assert_eq!(
        endpoint.queue_usage(),
        history,
        "native cleanup/snapshot do not reset queue high-water history"
    );
}

#[tokio::test]
async fn structural_queue_entry_capacity_overflow_rejects_cold_owner_before_driver_or_native_open()
{
    let fixture = Fixture::new().await;
    let mut grants = limits();
    grants.logical = NonZeroUsize::new(usize::MAX).unwrap();
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let failure = match DirectEndpointOwner::new(
        fixture.identity,
        crate::protocol_registry::UdpRuntimeServices::new(fixture.proxy.tcp_runtime_services())
            .network(),
        None,
        UdpReceiveQueueConfig::default(),
        grants,
        scope,
        None,
    ) {
        Err(error) => error,
        Ok(_) => panic!("structural packet bound arithmetic must not wrap"),
    };
    assert_eq!(failure.reason, Reason::Capacity);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert!(!failure.retry_permitted());
}

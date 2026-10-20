use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct QueueObservation {
    drops: AtomicUsize,
    changed: Notify,
}
impl zero_traits::IoObserver for QueueObservation {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_one();
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        assert_eq!(reason, zero_traits::PacketDropReason::QueueFull);
        self.dropped();
    }
}
#[tokio::test]
async fn configured_receive_capacity_reports_real_drop_and_cancel_drains_without_resource_shutdown()
{
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let observation = Arc::new(QueueObservation::default());
    let mut owner = fixture.owner(
        scope,
        UdpReceiveQueueConfig {
            capacity: 1,
            max_payload_bytes: Some(65_527),
            max_stack_payload_bytes: None,
        },
        Some(observation.clone()),
    );
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let cancel = CancellationToken::new();
    let (mut tx, mut rx, mut control) =
        fixture.open(&endpoint, peer.local_addr().unwrap(), cancel.clone());
    let local = send(&mut tx, &peer, b"request").await;
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
    peer.send_to(b"one", local).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let received = logical.changed.notified();
            tokio::pin!(received);
            received.as_mut().enable();
            if logical.bytes.load(Ordering::Acquire) == 3 {
                break;
            }
            received.await;
        }
    })
    .await
    .unwrap();
    peer.send_to(b"discarded", local).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), observation.changed.notified())
        .await
        .unwrap();
    assert_eq!(observation.drops.load(Ordering::Acquire), 1);
    assert_eq!(rx.recv().await.unwrap().unwrap().0, b"one");
    assert_eq!(logical.bytes.load(Ordering::Acquire), 0);
    // An actual caller token must wake the otherwise idle resource driver.
    cancel.cancel();
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
    tokio::net::UdpSocket::bind(local).await.unwrap();
    let (mut next_tx, _next_rx, mut next_control) = fixture.open(
        &endpoint,
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let next_local = send(&mut next_tx, &peer, b"second batch").await;
    assert!(next_control
        .wait_closed(deadline())
        .await
        .unwrap()
        .is_complete());
    tokio::net::UdpSocket::bind(next_local).await.unwrap();
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 2 original Direct UDP sockets"]
    );
}

#[tokio::test]
async fn cancel_drains_actual_queued_payload_before_reclaim_while_old_rx_stays_alive() {
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = fixture.owner(
        scope,
        UdpReceiveQueueConfig {
            capacity: 1,
            max_payload_bytes: Some(65_527),
            max_stack_payload_bytes: None,
        },
        None,
    );
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut old_receivers = Vec::new();
    let mut old_controls = Vec::new();
    for _ in 0..8 {
        let (mut tx, rx, mut control) = fixture.open(
            &endpoint,
            peer.local_addr().unwrap(),
            CancellationToken::new(),
        );
        let local = send(&mut tx, &peer, b"queue request").await;
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
        peer.send_to(b"queued payload", local).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let queued = logical.changed.notified();
                tokio::pin!(queued);
                queued.as_mut().enable();
                if logical.bytes.load(Ordering::Acquire) == b"queued payload".len() {
                    break;
                }
                queued.await;
            }
        })
        .await
        .unwrap();
        control.cancel(zero_platform_tokio::contract::CloseReason::Completed);
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_eq!(logical.bytes.load(Ordering::Acquire), 0);
        assert!(logical.receiver.lock().unwrap().is_empty());
        tokio::net::UdpSocket::bind(local).await.unwrap();
        old_receivers.push(rx);
        old_controls.push(control);
    }
    for mut old in old_receivers {
        assert!(old.recv().await.unwrap().is_none());
    }
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 8 original Direct UDP sockets"]
    );
    drop(old_controls);
}

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug, Default)]
struct BoundaryObserver {
    boundaries: AtomicUsize,
    panic: bool,
}
impl zero_traits::IoObserver for BoundaryObserver {
    fn datagram_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::AcqRel);
        assert!(
            !self.panic,
            "actual Direct datagram boundary callback panic"
        );
    }
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
fn observed_owner(fixture: &Fixture, observation: Arc<BoundaryObserver>) -> DirectEndpointOwner {
    let services = fixture
        .proxy
        .tcp_runtime_services()
        .with_io_observer(Some(observation));
    DirectEndpointOwner::new(
        fixture.identity,
        crate::protocol_registry::UdpRuntimeServices::new(services).network(),
        None,
        UdpReceiveQueueConfig::default(),
        limits(),
        TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        ),
        None,
    )
    .unwrap()
}
#[tokio::test]
async fn boundary_callback_panic_keeps_successfully_bound_original_in_same_owner() {
    let fixture = Fixture::new().await;
    let observation = Arc::new(BoundaryObserver {
        panic: true,
        ..Default::default()
    });
    let mut owner = observed_owner(&fixture, observation.clone());
    owner.start(deadline()).await.unwrap();
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (mut tx, _rx, _control) = fixture.open(
        &owner.endpoint(),
        target.local_addr().unwrap(),
        CancellationToken::new(),
    );
    assert!(tx
        .send_new_packet(
            &Address::Ipv4([127, 0, 0, 1]),
            &[target.local_addr().unwrap()],
            b"callback before send"
        )
        .await
        .is_err());
    let same = owner.shared.sockets.lock().await;
    assert_eq!(
        same.sockets.len(),
        1,
        "callback cannot consume the original socket out of its owner"
    );
    let original = same.sockets[0].socket.local_addr().unwrap();
    assert_eq!(observation.boundaries.load(Ordering::Acquire), 1);
    assert!(tokio::net::UdpSocket::bind(original).await.is_err());
    drop(same);
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    let mut source: &(dyn std::error::Error + 'static) = &report.failed_tasks[0].error;
    loop {
        if let Some(join) = source.downcast_ref::<tokio::task::JoinError>() {
            assert!(join.is_panic());
            break;
        }
        source = source
            .source()
            .expect("actual callback panic source retained");
    }
    tokio::net::UdpSocket::bind(original).await.unwrap();
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn failed_native_bind_does_not_count_an_uncreated_datagram_boundary() {
    let fixture = Fixture::new().await;
    // Keep the valid default dial policy. Publish only to this fixture's real
    // egress selector: the overlong name cannot identify a Linux interface.
    // Opening reaches bind_source and then the actual SO_BINDTODEVICE syscall,
    // which fails before a complete TokioDatagramSocket boundary exists.
    fixture.proxy.egress_interface.replace(Some(
        zero_platform_tokio::EgressInterface::new(
            "zero-fixture-interface-name-longer-than-ifnamsiz",
            u32::MAX,
        )
        .unwrap(),
    ));
    let observation = Arc::new(BoundaryObserver::default());
    let mut owner = observed_owner(&fixture, observation.clone());
    owner.start(deadline()).await.unwrap();
    let target = "192.0.2.197:12345".parse().unwrap();
    let (mut tx, _rx, _control) = fixture.open(&owner.endpoint(), target, CancellationToken::new());
    let failure = tx
        .send_new_packet(&Address::Ipv4([192, 0, 2, 197]), &[target], b"bind failure")
        .await
        .err()
        .expect("actual native interface bind must fail");
    assert!(failure.to_string().contains("bind_interface"), "{failure}");
    assert_eq!(observation.boundaries.load(Ordering::Acquire), 0);
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
}

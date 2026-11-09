use super::*;
use zero_platform_tokio::contract::lifecycle::{ResourceFailure, ResourceFailurePhase};
#[tokio::test]
async fn distinct_actual_resource_sources_are_strong_before_their_own_receipts_and_survive_retirement(
) {
    let proxy = zero_proxy::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (instance, _) = proxy.application_network_originals().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source_a = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let source_b = Arc::new(std::net::TcpListener::bind(held.local_addr().unwrap()).unwrap_err());
    let receipt_a = CleanupFailureReceipt::new();
    let receipt_b = CleanupFailureReceipt::new();
    assert!(receipt_a.bind_payload(source_a.clone()));
    assert!(receipt_b.bind_payload(source_b.clone()));
    let a = Arc::new(ResourceFailure::new(
        instance,
        ResourceFailurePhase::DeclaredResources,
        Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(source_a.clone())
                .with_cleanup_failure_receipt(receipt_a.clone()),
        ),
    ));
    let b = Arc::new(ResourceFailure::new(
        instance,
        ResourceFailurePhase::DeclaredResources,
        Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(source_b.clone())
                .with_cleanup_failure_receipt(receipt_b.clone()),
        ),
    ));
    let weak_a = Arc::downgrade(&a);
    let weak_b = Arc::downgrade(&b);
    let receiver = Arc::new(ReportReceiver::default());
    let mut first = CloseReport::default();
    first.resource_failures.push(a.clone());
    let first = receiver.receive("actual native graph", first);
    assert!(first.complete);
    let first = first.into_result().unwrap_err();
    assert!(receipt_a.is_acknowledged());
    assert!(
        !receipt_b.is_acknowledged(),
        "an earlier report ACK cannot accept a later actual source"
    );
    let mut later = CloseReport::default();
    later.resource_failures.extend([a.clone(), b.clone()]);
    let later = receiver.receive("actual native graph", later);
    assert!(later.complete);
    let later = later.into_result().unwrap_err();
    assert!(receipt_b.is_acknowledged());
    assert!(a.ownership_received() && b.ownership_received());
    assert_eq!(receiver.0.lock().unwrap().resources.len(), 2);
    drop(first);
    drop(a);
    drop(b);
    drop(receiver);
    drop(proxy);
    assert!(weak_a.upgrade().is_some() && weak_b.upgrade().is_some());
    drop(later);
    assert!(
        weak_a.upgrade().is_none() && weak_b.upgrade().is_none(),
        "source-only report receiver has no owner cycle"
    );
}

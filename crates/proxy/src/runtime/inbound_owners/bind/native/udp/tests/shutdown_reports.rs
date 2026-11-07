use super::*;
use tokio::{io::AsyncReadExt, time::Duration};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::scope::TaskScope;
#[tokio::test]
async fn matching_native_close_report_survives_an_independent_pending_io_timeout() {
    let proxy = crate::runtime::inbound_owners::bind::tests::proxy();
    let identity = proxy.protocols.allocate_resource_identity().unwrap();
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let declaration = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::MIN,
        max_pending_sends: NonZeroUsize::MIN,
    };
    let grant = Arc::new(
        NativeDatagramGrant::new_scoped(
            identity,
            &declaration,
            Some("127.0.0.1:0".parse().unwrap()),
            NonZeroUsize::MIN,
            CancellationToken::new(),
            scope.registrar(),
        )
        .unwrap()
        .with_original_budgets(proxy.protocols.runtime_host().original_budgets().clone()),
    );
    let view = grant.bind().unwrap();
    drop(view);
    let actual_report = grant
        .native
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .shutdown(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert!(actual_report.is_complete());
    assert_eq!(actual_report.closed_io.len(), 1);
    assert!(grant.persist_shutdown_report(Ok(actual_report)).is_none());
    let (mut pending, _peer) = tokio::io::duplex(1);
    let mut bytes = [0];
    let elapsed = tokio::time::timeout(Duration::from_millis(1), pending.read_exact(&mut bytes))
        .await
        .unwrap_err();
    let failure =
        ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component).with_source(elapsed);
    let returned = grant.persist_shutdown_report(Err(failure)).unwrap();
    assert_eq!(returned.reason, Reason::Timeout);
    assert_eq!(
        grant.shutdown_reports.lock().unwrap().closed_io.len(),
        1,
        "the independent failed branch cannot discard an already produced native receipt"
    );
    let mut resource = DatagramResource(grant.clone());
    let report = resource
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(grant.shutdown_reports.lock().unwrap().closed_io.is_empty());
    assert!(resource
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .closed_io
        .is_empty());
    assert!(scope
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_complete());
}

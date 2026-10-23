use super::*;
use zero_platform_tokio::contract::scope::SpawnError;
fn original_rejection(error: &ChannelError) -> &DriverRegistrationFailure {
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(rejection) = source.downcast_ref::<DriverRegistrationFailure>() {
            return rejection;
        }
        source = source
            .source()
            .expect("original typed registration refusal survives");
    }
}
#[tokio::test]
async fn actual_late_scope_capacity_refusal_is_delivered_without_stranded_connecting_or_retry() {
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let (mut owner, mut receiver, handle, retained) = owner(&scope, 3);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut wire = [0; 2048];
    let mut receiving = Box::pin(peer.recv_from(&mut wire));
    poll_fn(|cx| {
        assert!(receiving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let failure = handle
        .connect(peer.local_addr().unwrap(), "localhost")
        .await
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::Capacity);
    assert!(!failure.retry_permitted());
    assert_eq!(
        original_rejection(&failure).reason,
        DriverRegistrationReason::Scope(SpawnError::Capacity)
    );
    assert_eq!(owner.active_users(), 0);
    assert_eq!(
        owner.driver_tasks().len(),
        1,
        "no invented task for the unaccepted connection driver"
    );
    poll_fn(|cx| {
        assert!(receiving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let again = handle
        .connect(peer.local_addr().unwrap(), "localhost")
        .await
        .err()
        .unwrap();
    assert!(
        std::ptr::eq(original_rejection(&failure), original_rejection(&again)),
        "unrelated retry cannot erase the same sticky original source"
    );
    assert!(retained.local_addr().is_ok());
    let (report, classes) = tokio::join!(
        owner.shutdown(Instant::now() + Duration::from_secs(2)),
        collect(&mut scope, &mut receiver, 1),
    );
    assert_eq!(classes, [TaskClass::Critical]);
    let report = report.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.drivers.rejection.unwrap().reason,
        DriverRegistrationReason::Scope(SpawnError::Capacity)
    );
    assert_eq!(report.confirmed_main, 1);
}
#[tokio::test]
async fn actual_cold_start_refusal_keeps_original_native_owner_until_explicit_no_task_cleanup() {
    let scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(bound(1)));
    let native = socket();
    let retained = native.socket();
    let (mut owner, _receiver) = OwnedEndpointOwner::new(
        native,
        scope.registrar(),
        OwnedEndpointLimits {
            drivers: bound(1),
            users: bound(1),
        },
    );
    let (server, client) = configs();
    let failure = owner
        .start(Default::default(), Some(server), Some(client))
        .err()
        .unwrap();
    assert_eq!(
        original_rejection(&failure).reason,
        DriverRegistrationReason::Scope(SpawnError::NotStarted)
    );
    assert!(owner.driver_tasks().is_empty());
    assert!(retained.local_addr().is_ok());
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(
        report.joined.is_empty(),
        "a rejected future cannot produce a successful TaskExit"
    );
    assert_eq!(report.confirmed_main, 1);
    assert_eq!(
        retained.local_addr().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

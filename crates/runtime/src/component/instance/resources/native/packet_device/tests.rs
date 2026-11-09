//! Actual original driver registration, native acquisition failure and close gates.
use super::*;
use std::{
    future::Future, num::NonZeroUsize, os::fd::AsRawFd, pin::Pin, task::Poll, time::Duration,
};
use tokio::{io::ReadBuf, sync::oneshot};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::scope::{HostedScope, TaskClass, TaskScope};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn scope() -> HostedScope {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    scope
}
fn owner(scope: &HostedScope, fd: i32, interface: &str) -> PacketDeviceOwner {
    owner_with_budget(scope, fd, interface, Arc::default())
}
fn owner_with_budget(
    scope: &HostedScope,
    fd: i32,
    interface: &str,
    roots: Arc<crate::HostOriginalBudgets>,
) -> PacketDeviceOwner {
    PacketDeviceOwner::new(
        0,
        ComponentInstanceId {
            component: ComponentId(7),
            generation: ResourceGeneration(1),
        },
        scope.registrar().scope_identity(),
        "native-test".into(),
        PacketDeviceBinding::Descriptor {
            fd,
            interface: interface.into(),
        },
        CancellationToken::new(),
        roots,
        Some(scope.registrar().downgrade()),
    )
    .unwrap()
}
// This is the real device-loop future: bootstrap transfers its unique native
// I/O; the next branch polls that original I/O, never a marker worker.
async fn original_driver(
    mut input: oneshot::Receiver<GrantedPacketDevice>,
    mut bootstrap: Option<oneshot::Sender<()>>,
    mut reading: Option<oneshot::Sender<()>>,
    stop: CancellationToken,
) -> Result<(), ChannelError> {
    let grant = std::future::poll_fn(|cx| {
        let poll = Pin::new(&mut input).poll(cx);
        if poll.is_pending() {
            if let Some(ready) = bootstrap.take() {
                let _ = ready.send(());
            }
        }
        poll
    })
    .await;
    let Ok(mut grant) = grant else { return Ok(()) };
    let mut bytes = [0; 65536];
    let result = std::future::poll_fn(|cx| {
        let mut buffer = ReadBuf::new(&mut bytes);
        let result = grant.io.as_mut().poll_read(cx, &mut buffer);
        if result.is_pending() {
            if let Some(ready) = reading.take() {
                let _ = ready.send(());
            }
        }
        result
    });
    tokio::select! {biased;_=stop.cancelled()=>Ok(()),result=result=>result.map_err(|source|error(ComponentId(7),Reason::Io,"actual original read failed").with_source(source))}
}
fn driver(
    scope: &HostedScope,
) -> (
    oneshot::Sender<GrantedPacketDevice>,
    oneshot::Receiver<()>,
    TaskJoinWitness,
) {
    let (send, input) = oneshot::channel();
    let (pending, observed) = oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(
            TaskClass::Critical,
            original_driver(input, Some(pending), None, scope.registrar().cancellation()),
        )
        .unwrap();
    (send, observed, witness)
}
async fn joined(scope: &mut HostedScope, witness: &TaskJoinWitness) {
    let receipt = tokio::time::timeout_at(deadline(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    assert!(receipt.result().is_ok());
    scope.acknowledge(&receipt).unwrap();
}
fn failed(result: Result<GrantedPacketDevice, ChannelError>) -> ChannelError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("unexpected native view"),
    }
}
fn acquisition(error: &ChannelError) -> &zero_tun::TunAcquisitionError {
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(original) = source.downcast_ref::<zero_tun::TunAcquisitionError>() {
            return original;
        }
        source = source.source().expect("typed original acquisition source");
    }
}
#[tokio::test]
async fn foreign_scope_and_already_joined_registration_never_attempt_native_acquisition() {
    let mut actual = scope();
    let mut foreign = scope();
    let original = owner(&actual, i32::MAX, "not-a-tun");
    let (send, pending, wrong) = driver(&foreign);
    pending.await.unwrap();
    assert_eq!(
        failed(original.open(wrong.clone()).await).reason,
        Reason::Revoked
    );
    assert!(!original.state.lock().unwrap().attempted);
    assert!(matches!(
        original.original.outcome(),
        TunAcquisitionOutcome::KnownNoNative
    ));
    drop(send);
    joined(&mut foreign, &wrong).await;
    foreign.begin_shutdown();
    assert!(foreign.next_receipt().await.is_none());
    let (send, pending, done) = driver(&actual);
    pending.await.unwrap();
    drop(send);
    joined(&mut actual, &done).await;
    assert_eq!(failed(original.open(done).await).reason, Reason::Revoked);
    assert!(!original.state.lock().unwrap().attempted);
    assert!(original.shutdown(deadline()).await.is_complete());
    actual.begin_shutdown();
    assert!(actual.next_receipt().await.is_none());
    assert!(actual.is_drained());
}
#[tokio::test]
async fn known_no_native_failure_retains_the_pending_original_driver_and_cancelled_shutdown() {
    let mut actual = scope();
    let original = owner(&actual, i32::MAX, "not-a-tun");
    let retained = original.original.clone();
    let (send, pending, witness) = driver(&actual);
    pending.await.unwrap();
    let failure = failed(original.open(witness.clone()).await);
    assert!(matches!(
        acquisition(&failure).outcome,
        TunAcquisitionOutcome::KnownNoNative
    ));
    let source = acquisition(&failure).source.clone();
    assert!(Arc::ptr_eq(
        &source,
        &retained.acquisition_failure().unwrap()
    ));
    assert_eq!(
        failed(original.open(witness.clone()).await).reason,
        Reason::Revoked
    );
    assert!(original
        .state
        .lock()
        .unwrap()
        .driver
        .as_ref()
        .unwrap()
        .same_registration(&witness));
    let mut closing = Box::pin(original.shutdown(deadline()));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(Arc::ptr_eq(&retained, &original.original));
    assert!(!witness.is_joined());
    assert!(!original.evidence().physical_confirmed());
    // Acquisition failure closes the original bootstrap input. Only the real
    // host join consumer below may complete this same driver registration.
    drop(send);
    joined(&mut actual, &witness).await;
    let report = original.shutdown(deadline()).await;
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert!(original.evidence().physical_confirmed());
    assert!(Arc::ptr_eq(
        &source,
        &retained.acquisition_failure().unwrap()
    ));
    actual.begin_shutdown();
    assert!(actual.next_receipt().await.is_none());
    assert!(actual.is_drained());
}
#[tokio::test]
async fn invalid_device_validation_preserves_the_actual_duplicate_close_and_borrowed_file() {
    let borrowed = tempfile::tempfile().unwrap();
    let mut actual = scope();
    let original = owner(&actual, borrowed.as_raw_fd(), "not-a-tun");
    let (send, pending, witness) = driver(&actual);
    pending.await.unwrap();
    let failure = failed(original.open(witness.clone()).await);
    let TunAcquisitionOutcome::Closed(receipt) = &acquisition(&failure).outcome else {
        panic!("actual invalid-device duplicate was explicitly closed");
    };
    let TunAcquisitionOutcome::Closed(retained) = original.original.outcome() else {
        panic!("same controller retains original close");
    };
    assert!(Arc::ptr_eq(receipt, &retained));
    assert!(!original.evidence().physical_confirmed());
    drop(send);
    joined(&mut actual, &witness).await;
    let report = original.shutdown(deadline()).await;
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(original.evidence().physical_confirmed());
    assert!(borrowed.metadata().is_ok());
    actual.begin_shutdown();
    assert!(actual.next_receipt().await.is_none());
    assert!(actual.is_drained());
}
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires /dev/net/tun and CAP_NET_ADMIN; creates only one dedicated ephemeral TUN"]
async fn actual_tun_pending_read_joins_original_driver_before_matching_duplicate_close() {
    use zero_tun::{OwnedTunDevice, TunDevice};
    let borrowed = zero_tun::LinuxTun::create(Some("zr2packet%d")).unwrap();
    let mut actual = scope();
    let original = owner(&actual, borrowed.as_raw_fd(), borrowed.name());
    let (send, input) = oneshot::channel();
    let (boot, boot_pending) = oneshot::channel();
    let (read, read_pending) = oneshot::channel();
    let (_, witness) = actual
        .registrar()
        .spawn_tracked(
            TaskClass::Critical,
            original_driver(
                input,
                Some(boot),
                Some(read),
                actual.registrar().cancellation(),
            ),
        )
        .unwrap();
    boot_pending.await.unwrap();
    let grant = original
        .open(witness.clone())
        .await
        .unwrap_or_else(|error| panic!("actual dedicated TUN: {error}"));
    let provider = grant.original.clone();
    assert!(original
        .original_registration()
        .unwrap()
        .same_owner(&provider));
    assert!(provider.device().same_provider(&grant.identity));
    assert!(provider.driver().same_registration(&witness));
    let origin =
        zero_platform_tokio::controlled_carrier::CarrierOrigin::from_packet(provider.clone());
    assert!(origin.closed().is_none());
    assert!(send.send(grant).is_ok());
    read_pending.await.unwrap();
    assert!(original.state.lock().unwrap().view_live);
    assert!(!original.evidence().physical_confirmed());
    let mut closing = Box::pin(original.shutdown(deadline()));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(!witness.is_joined());
    assert!(matches!(
        original.original.outcome(),
        TunAcquisitionOutcome::Unknown(Some(_))
    ));
    actual.begin_shutdown();
    joined(&mut actual, &witness).await;
    let report = original.shutdown(deadline()).await;
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(original.evidence().physical_confirmed());
    let native = origin
        .closed()
        .expect("same explicit original native TUN close");
    assert!(native.matches(&origin));
    assert!(native.had_native_acquisition());
    assert!(provider.closed().unwrap().matches(&provider));
    assert!(provider
        .native()
        .closed()
        .unwrap()
        .matches(provider.native()));
    assert!(provider.driver().is_joined());
    assert!(actual.next_receipt().await.is_none());
    assert!(actual.is_drained());
    let identity = borrowed.owned_identity();
    let receipt = borrowed.close_owned().unwrap();
    assert!(receipt.identity().same_owner(&identity));
}

#[path = "tests/budget.rs"]
mod budget;

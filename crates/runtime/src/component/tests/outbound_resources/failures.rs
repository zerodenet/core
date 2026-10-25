use super::*;
use std::error::Error;

fn retains(error: &ChannelError, original: &Arc<ChannelError>) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<Arc<ChannelError>>())
        .is_some_and(|retained| Arc::ptr_eq(retained, original))
}

#[tokio::test]
async fn real_network_invalidation_consumes_one_attempt_but_proves_no_native_socket_was_acquired() {
    let target = "127.0.0.1:53".parse().unwrap();
    let control = EgressInterfaceControl::default();
    let grants = provider(target, &control, CancellationToken::new());
    let req = request(target, &control);
    let (mut scope, task) = binding(tokio::runtime::Handle::current());
    grants.activate(task).unwrap();
    control.invalidate_network();
    assert_eq!(
        grants.open_outbound_datagram(req).unwrap_err().reason,
        Reason::StalePlan
    );
    let fault = grants.fault().unwrap();
    assert!(!grants.native_unconfirmed());
    assert!(grants.state().attempted);
    assert!(retains(
        &grants.open_outbound_datagram(req).unwrap_err(),
        &fault
    ));
    assert!(scope
        .shutdown(deadline())
        .await
        .exited_tasks
        .contains(&task));
    let report = grants.shutdown_io().unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert!(Arc::ptr_eq(&grants.fault().unwrap(), &fault));
}

#[test]
fn actual_tokio_registration_error_keeps_its_source_and_permanent_unknown_native_outcome() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let (mut scope, task) = binding(handle.clone());
    let target = "127.0.0.1:53".parse().unwrap();
    let control = EgressInterfaceControl::default();
    let grants = provider(target, &control, CancellationToken::new());
    let req = request(target, &control);
    grants.activate(task).unwrap();
    // Keep the real runtime handle but synchronously shut down its actual I/O
    // driver. from_std must return the real registration failure after bind.
    drop(runtime);
    let entered = handle.enter();
    let failure = grants.open_outbound_datagram(req).unwrap_err();
    assert_eq!(failure.reason, Reason::Io);
    assert!(grants.native_unconfirmed());
    let fault = grants.fault().unwrap();
    assert!(retains(&failure, &fault));
    let mut source: &(dyn Error + 'static) = &failure;
    let mut io_errors = 0;
    while let Some(next) = source.source() {
        if let Some(native) = next.downcast_ref::<std::io::Error>() {
            assert!(!native.to_string().is_empty());
            io_errors += 1;
        }
        source = next;
    }
    assert!(
        io_errors >= 2,
        "context and the actual original registration error remain accessible"
    );
    assert!(retains(
        &grants.open_outbound_datagram(req).unwrap_err(),
        &fault
    ));
    for _ in 0..2 {
        let failure = grants.shutdown_io().unwrap_err();
        assert_eq!(failure.stage, Stage::Close);
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        assert!(retains(&failure, &fault));
        assert!(grants.native_unconfirmed());
    }
    drop(entered);
    let cleanup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(cleanup
        .block_on(scope.shutdown(deadline()))
        .exited_tasks
        .contains(&task));
}

#[test]
fn actual_registration_panic_retains_unknown_state_and_the_real_task_join_error() {
    // A real Tokio runtime without its I/O driver makes registration panic after
    // acquiring the native socket. No synthetic failure hook or hidden job.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mut scope = TaskScope::new(runtime.handle().clone(), NonZeroUsize::new(1).unwrap());
    let control = EgressInterfaceControl::default();
    let target = "127.0.0.1:53".parse().unwrap();
    let grants = provider(target, &control, CancellationToken::new());
    let actual = grants.clone();
    let req = request(target, &control);
    let task = scope
        .registrar()
        .spawn(TaskClass::Critical, async move {
            let _socket = actual.open_outbound_datagram(req)?;
            Ok(())
        })
        .unwrap();
    grants.activate(task).unwrap();
    let exit = runtime.block_on(scope.next_exit()).unwrap();
    assert_eq!(exit.id, task);
    let failure = exit.result.unwrap_err();
    let join = failure
        .source()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap();
    assert!(join.is_panic());
    assert!(
        grants.native_unconfirmed(),
        "unwinding never clears the installed native outcome"
    );
    let fault = grants.fault().unwrap();
    assert_eq!(fault.reason, Reason::Internal);
    assert!(retains(&grants.shutdown_io().unwrap_err(), &fault));
    assert!(runtime.block_on(scope.shutdown(deadline())).is_complete());
}

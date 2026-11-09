use super::*;
use crate::component::instance::SharedError;
use futures_util::FutureExt;
use zero_platform_tokio::contract::ChannelError;

fn original(error: &EngineError) -> SharedError {
    let EngineError::Io(error) = error else {
        panic!("original retained ChannelError")
    };
    let error = error
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    std::error::Error::source(error)
        .unwrap()
        .downcast_ref::<SharedError>()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn promotion_failure_retains_native_owner_and_exact_fault_until_confirmed_source_handoff() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut old = lease(&proxy, &config, 0);
    old.bind_tcp().unwrap();
    let (operation, old_view, old_install) = promote_tcp(old.finish().unwrap());
    drop(operation);
    let old_owner = old_install.owner();
    old_owner.seal_listener_installation();
    let old_address = old_view.local_addr().unwrap();

    let mut candidate = lease(&proxy, &config, 0);
    let candidate_owner = candidate.owner();
    let identity = candidate_owner.identity();
    candidate.bind_tcp().unwrap();
    let candidate = candidate.finish().unwrap();
    let address = candidate.local_addr().unwrap();
    let error = match candidate.promote_definition() {
        Err(error) => error,
        Ok(_) => panic!("old active native holder must be confirmed first"),
    };
    let failure = original(&error);
    assert!(TcpListener::bind(address).await.is_err());
    assert!(TcpListener::bind(old_address).await.is_err());
    assert!(!old_owner.task_access().cancellation().is_cancelled());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    let interrupted = proxy
        .inbound_owners
        .retire_bind_reservation_reported(
            "first",
            Instant::now() + Duration::from_secs(2),
            |terminal| {
                assert_eq!(terminal.identity(), identity);
                assert_eq!(terminal.joined_tasks(), 0);
                assert_eq!(terminal.closed_io_receipts(), 1);
                assert!(std::ptr::eq::<zero_platform_tokio::contract::ChannelError>(
                    (&terminal.failure().unwrap()).as_ref(),
                    (&failure).as_ref()
                ));
                Err(std::io::Error::other("supervisor receipt delivery failed").into())
            },
        )
        .await
        .unwrap_err();
    assert!(interrupted
        .to_string()
        .contains("supervisor receipt delivery failed"));
    assert!(proxy.inbound_owners.identities().contains(&identity));
    let replacement = TcpListener::bind(address).await.unwrap();
    let panic =
        std::panic::AssertUnwindSafe(proxy.inbound_owners.retire_bind_reservation_reported(
            "first",
            Instant::now() + Duration::from_secs(2),
            |terminal| {
                assert!(std::ptr::eq::<zero_platform_tokio::contract::ChannelError>(
                    (&terminal.failure().unwrap()).as_ref(),
                    (&failure).as_ref()
                ));
                panic!("actual supervisor publication interrupted");
            },
        ))
        .catch_unwind()
        .await;
    assert!(panic.is_err());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    let released = proxy
        .inbound_owners
        .retire_bind_reservation_reported(
            "first",
            Instant::now() + Duration::from_secs(2),
            |terminal| {
                assert_eq!(terminal.closed_io_receipts(), 1);
                assert!(std::ptr::eq::<zero_platform_tokio::contract::ChannelError>(
                    (&terminal.failure().unwrap()).as_ref(),
                    (&failure).as_ref()
                ));
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        crate::runtime::inbound_owners::released_fault_identity(&released),
        Some(identity)
    );
    assert!(!proxy.inbound_owners.identities().contains(&identity));
    assert_eq!(
        proxy.inbound_owners.owner("first").unwrap().identity(),
        old_owner.identity()
    );
    assert_eq!(replacement.local_addr().unwrap(), address);
    proxy
        .inbound_owners
        .shutdown_all_reported(Instant::now() + Duration::from_secs(2), |_, _| Ok(()))
        .await
        .unwrap();
    let _old_closed = TcpListener::bind(old_address).await.unwrap();
    drop(old_view);
}

#[tokio::test]
async fn shutdown_gate_rejects_late_promotion_without_fault_and_reopens_only_after_native_proof() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut candidate = lease(&proxy, &config, 0);
    let identity = candidate.owner().identity();
    candidate.bind_tcp().unwrap();
    let candidate = candidate.finish().unwrap();
    let address = candidate.local_addr().unwrap();
    proxy.inbound_owners.close_bind_admission();
    assert!(proxy.inbound_owners.open_bind_admission().is_err());
    let stopped = match candidate.promote_definition() {
        Err(error) => error,
        Ok(_) => panic!("shutdown must fence promotion before owner snapshots"),
    };
    assert!(crate::runtime::inbound_owners::preparation_stopped(
        &stopped
    ));
    assert!(proxy.inbound_owners.owner("first").is_none());
    assert!(TcpListener::bind(address).await.is_err());
    proxy
        .inbound_owners
        .retire_bind_reservation_reported(
            "first",
            Instant::now() + Duration::from_secs(2),
            |terminal| {
                assert_eq!(terminal.identity(), identity);
                assert_eq!(terminal.joined_tasks(), 0);
                assert_eq!(terminal.closed_io_receipts(), 1);
                assert!(terminal.failure().is_none());
                Ok(())
            },
        )
        .await
        .unwrap();
    let _closed_original = TcpListener::bind(address).await.unwrap();
    proxy.inbound_owners.open_bind_admission().unwrap();
    let mut next = lease(&proxy, &config, 0);
    next.bind_tcp().unwrap();
    drop(next);
    proxy
        .inbound_owners
        .shutdown_all_reported(Instant::now() + Duration::from_secs(2), |_, _| Ok(()))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_published_closed_fault_cannot_hide_another_original_pending_scope() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let first = lease(&proxy, &config, 0);
    let closed = first.owner();
    let original_first = first.context().listen_tcp().unwrap();
    let source = TcpStream::connect(("127.0.0.1", 0)).await.unwrap_err();
    first.reject(source.into());
    drop(first);
    assert!(closed
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert_eq!(closed.terminal_for_test().unwrap().closed_io_receipts(), 1);

    let second = lease(&proxy, &config, 1);
    let pending_owner = second.owner();
    let identity = pending_owner.identity();
    let original_second = second.context().listen_tcp().unwrap();
    // The declared accepted graph correctly cancels its own reads. Keep the
    // actual Scope suspended instead on independent fixture I/O; closing the
    // original listener cannot stand in for joining this real registered task.
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(gate.local_addr().unwrap())
        .await
        .unwrap();
    let (mut stream, _) = gate.accept().await.unwrap();
    let entered = Arc::new(Notify::new());
    let pending = entered.clone();
    pending_owner
        .spawn_driver(async move {
            let read = stream.read_u8();
            tokio::pin!(read);
            let value = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => {
                    pending.notify_one();
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await?;
            assert_eq!(value, 42);
            Ok(())
        })
        .unwrap();
    pending_owner.seal_listener_installation();
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    drop(second);
    let error = proxy
        .inbound_owners
        .retire_bind_reservations_reported(Instant::now(), |_, _| Ok(()))
        .await
        .unwrap_err();
    assert!(
        crate::runtime::inbound_owners::released_fault_identity(&error).is_none(),
        "a complete first fault must not mask actual unconfirmed native/scope cleanup"
    );
    assert!(original_first.local_addr().is_err());
    assert!(pending_owner.terminal_for_test().is_none());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    client.write_u8(42).await.unwrap();
    proxy
        .inbound_owners
        .retire_bind_reservations_reported(Instant::now() + Duration::from_secs(2), |_, _| Ok(()))
        .await
        .unwrap();
    let terminal = pending_owner.terminal_for_test().unwrap();
    assert_eq!(terminal.closed_io_receipts(), 1);
    assert_eq!(terminal.joined_tasks(), 1);
    assert!(original_second.local_addr().is_err());
}

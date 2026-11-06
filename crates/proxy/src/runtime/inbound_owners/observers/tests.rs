use super::*;
use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};

#[tokio::test]
async fn parking_preserves_the_original_observer_join_handle_and_panic() {
    let registry = InboundOwnerRegistry::default();
    let mut original = registry.take_listener_observers().unwrap();
    assert!(registry.take_listener_observers().is_err());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut remote = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut io, _) = listener.accept().await.unwrap();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let handle = original.spawn(async move {
        let mut entered = Some(entered);
        let mut byte = [0];
        let read = io.read(&mut byte);
        tokio::pin!(read);
        std::future::poll_fn(|cx| {
            let result = read.as_mut().poll(cx);
            if result.is_pending() {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                }
            }
            result
        })
        .await
        .unwrap();
        panic!("same parked observer panic");
    });
    pending.await.unwrap(); // The actual original native read is Pending.
    drop(original);
    remote.write_u8(1).await.unwrap();
    let mut resumed = registry.take_listener_observers().unwrap();
    assert!(resumed.has_previous_generation());
    let panic = timeout(Duration::from_secs(2), resumed.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(panic.id(), handle.id());
    assert!(panic.is_panic());
    assert_eq!(
        panic.into_panic().downcast_ref::<&str>(),
        Some(&"same parked observer panic")
    );
    resumed.previous_generation_retired();
    drop(resumed);
    let empty = registry.take_listener_observers().unwrap();
    assert!(empty.is_empty());
    assert!(!empty.has_previous_generation());
}

#[test]
fn an_interrupted_retirement_keeps_the_exact_current_fault_without_history() {
    let registry = InboundOwnerRegistry::default();
    let mut original = registry.take_listener_observers().unwrap();
    original.retain_failure(EngineError::NoInbounds);
    drop(original);
    let mut resumed = registry.take_listener_observers().unwrap();
    assert!(resumed.has_previous_generation());
    assert!(matches!(
        resumed.take_failure(),
        Some(EngineError::NoInbounds)
    ));
    assert!(resumed.take_failure().is_none());
    resumed.previous_generation_retired();
    drop(resumed);
    assert!(
        !registry
            .take_listener_observers()
            .unwrap()
            .has_previous_generation()
    );
}

mod wake;

#[tokio::test]
async fn empty_observer_set_preserves_pre_observer_native_acquisition_until_real_cleanup() {
    let proxy = super::super::bind::tests::proxy();
    let config = proxy.engine().config();
    let observers = proxy.inbound_owners.take_listener_observers().unwrap();
    assert!(!observers.has_previous_generation());
    let reservation = super::super::bind::tests::lease(&proxy, &config, 0);
    let identity = reservation.owner().identity();
    let view = reservation.context().listen_tcp().unwrap();
    let original = view.local_addr().unwrap();
    assert!(TcpListener::bind(original).await.is_err());
    drop(view); // Revoke only; host retains the exact original FD/control.
    drop(reservation); // No observer was ever installed or TaskId manufactured.
    assert!(observers.is_empty());
    drop(observers);
    let mut resumed = proxy.inbound_owners.take_listener_observers().unwrap();
    assert!(resumed.is_empty());
    assert!(
        resumed.has_previous_generation(),
        "native acquisition dirtied the original batch before any observer"
    );
    assert!(
        proxy
            .inbound_owners
            .confirm_observers_retired(&mut resumed)
            .is_err()
    );
    assert!(TcpListener::bind(original).await.is_err());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    let mut published = false;
    proxy
        .inbound_owners
        .shutdown_all_reported(Instant::now() + Duration::from_secs(2), |tag, terminal| {
            assert_eq!(tag, "first");
            assert_eq!(terminal.identity(), identity);
            assert_eq!(
                terminal.joined_tasks(),
                0,
                "no fake driver exit for a cold acquisition"
            );
            assert_eq!(terminal.closed_io_receipts(), 1);
            assert!(terminal.failure().is_none());
            published = true;
            Ok(())
        })
        .await
        .unwrap();
    assert!(published);
    proxy
        .inbound_owners
        .confirm_observers_retired(&mut resumed)
        .unwrap();
    assert!(!resumed.has_previous_generation());
    drop(resumed);
    assert!(
        !proxy
            .inbound_owners
            .take_listener_observers()
            .unwrap()
            .has_previous_generation()
    );
    let _replacement = TcpListener::bind(original).await.unwrap();
}

#[tokio::test]
async fn dynamic_source_enters_the_original_empty_observer_consumer() {
    let proxy = super::super::bind::tests::proxy();
    let mut observers = proxy.inbound_owners.take_listener_observers().unwrap();
    assert!(observers.is_empty());
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install_listener("dynamic-source", &proxy.protocols, 1, stop)
        .unwrap();
    let owner = installed.owner();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut remote = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut io, _) = listener.accept().await.unwrap();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let access = owner.task_access();
    let (main_sent, main_received) =
        tokio::sync::oneshot::channel::<zero_platform_tokio::contract::scope::TaskJoinWitness>();
    owner
        .spawn_driver(async move {
            #[cfg(feature = "raw-ip-runtime")]
            main_sent
                .send(
                    access
                        .source_main_witness()
                        .await
                        .map_err(std::io::Error::other)?,
                )
                .unwrap();
            #[cfg(not(feature = "raw-ip-runtime"))]
            let _ = (access, main_sent);

            let mut entered = Some(entered);
            let read = io.read_u8();
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                    }
                }
                result
            })
            .await?;
            Err(EngineError::NoInbounds)
        })
        .unwrap();
    owner.seal_listener_installation();
    pending.await.unwrap();
    #[cfg(feature = "raw-ip-runtime")]
    let actual_main = main_received.await.unwrap();
    #[cfg(not(feature = "raw-ip-runtime"))]
    let _ = main_received;
    #[cfg(feature = "raw-ip-runtime")]
    let original_main_task = actual_main.task_id().expect("actual registered main task");
    let waiting = observers.join_next_running();
    tokio::pin!(waiting);
    assert!(futures_util::poll!(&mut waiting).is_pending());
    proxy
        .inbound_owners
        .observers
        .enqueue_system_tun(owner.clone())
        .unwrap();
    remote.write_u8(1).await.unwrap();
    assert!(
        timeout(Duration::from_secs(2), &mut waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        *stopping.borrow_and_update(),
        "actual observer consumed the fault and stopped its owner"
    );
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("dynamic-source", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    #[cfg(feature = "raw-ip-runtime")]
    assert_eq!(
        actual_main
            .try_joined()
            .expect("actual source main joined through original observer")
            .id(),
        original_main_task
    );
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert!(receipt.terminal().failure().is_some());
}

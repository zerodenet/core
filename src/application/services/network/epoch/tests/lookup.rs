//! The actual blocking worker and original Scope join survive waiter Drop.
use super::*;
use crate::application::services::network::unique_candidates;
use std::{future::poll_fn, num::NonZeroUsize, sync::Condvar, task::Poll};
use tokio::io::AsyncWriteExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_lookup_keeps_actual_blocking_join_capacity_until_the_original_worker_returns() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let epoch = epoch(0, EpochLimits::new(2, 1, 1).unwrap());
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        struct Release(Arc<(Mutex<bool>, Condvar)>);
        impl Drop for Release {
            fn drop(&mut self) {
                *self.0 .0.lock().unwrap() = true;
                self.0 .1.notify_all();
            }
        }
        let release = Release(gate.clone());
        let (entered, working) = oneshot::channel();
        let mut lookup = Box::pin(epoch.lookup(move || {
            use std::net::ToSocketAddrs;
            let result = ("localhost", 443).to_socket_addrs().map(unique_candidates);
            // Real system resolution has run on the actual blocking worker;
            // keep that same worker live to test its original join obligation.
            entered.send(()).unwrap();
            let _released = gate
                .1
                .wait_while(gate.0.lock().unwrap(), |released| !*released)
                .unwrap();
            result
        }));
        poll_fn(|cx| {
            assert!(lookup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        working.await.unwrap();
        let original = epoch.lookups.lock().unwrap()[0].clone();
        let witness = original.witness.get().unwrap().clone();
        assert!(!witness.is_joined());
        drop(lookup);
        assert!(!original.caller_live.load(Ordering::Acquire));
        let denied = epoch
            .lookup(|| panic!("capacity denial started another native lookup"))
            .await
            .unwrap_err();
        assert_eq!(denied.kind(), io::ErrorKind::WouldBlock);
        assert!(epoch
            .retire(Instant::now() + std::time::Duration::from_millis(20))
            .await
            .is_err());
        assert!(!witness.is_joined());
        assert!(!epoch.closed());
        assert_eq!(epoch.lookups.lock().unwrap().len(), 1);
        drop(release);
        epoch.retire(end()).await.unwrap();
        assert!(witness.is_joined());
        assert!(witness.try_joined().unwrap().registration_released());
        assert!(epoch.lookups.lock().unwrap().is_empty());
        assert!(epoch.closed());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_lookup_is_registered_on_the_same_epoch_scope_and_does_not_create_a_nested_join() {
    let epoch = epoch(0, EpochLimits::default());
    let values = tokio::select! {
        result = epoch.resolve("localhost".to_owned(), 443) => result.unwrap(),
        _ = epoch.drive() => panic!("lookup Scope stopped before its actual join"),
    };
    assert!(!values.is_empty());
    assert!(values
        .iter()
        .all(|value| value.ip().is_loopback() && value.port() == 443));
    assert!(epoch.lookups.lock().unwrap().is_empty());
    epoch.retire(end()).await.unwrap();
    assert!(epoch.closed());
}

#[cfg(feature = "event-dispatcher")]
#[tokio::test]
async fn physically_retired_epoch_fault_retains_original_task_sources_and_allows_a_new_same_endpoint_generation(
) {
    let network = super::super::super::ApplicationEventSinkTcpDialer::for_test(
        EgressInterfaceControl::default(),
    );
    network
        .originals
        .install_after_retirement(NonZeroUsize::new(1).unwrap())
        .unwrap();
    network.begin_generation().unwrap();
    let epoch = network.current().unwrap();
    let witness = epoch
        .spawn(Box::pin(async { panic!("original Epoch worker failure") }))
        .unwrap();
    let fault = tokio::time::timeout_at(end(), async {
        loop {
            if let Err(fault) = network.prepare_request_owned(end()).await {
                break fault;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(epoch.closed());
    assert!(network.retirement_complete());
    assert!(witness.is_joined());
    let joined = witness.try_joined().unwrap();
    assert!(joined.result().is_err());
    assert!(joined.registration_released());
    assert!(
        epoch
            .failures
            .lock()
            .unwrap()
            .iter()
            .any(|exit| { exit.id == witness.task_id().unwrap() && exit.result.is_err() }),
        "ordinary prepare reaped and retained the same actual task exit before shutdown"
    );
    let actual = joined.result().unwrap_err();
    let mut source = Some(&fault as &(dyn std::error::Error + 'static));
    let mut retained_actual = false;
    while let Some(current) = source {
        if let Some(original) =
            current.downcast_ref::<zero_platform_tokio::contract::ChannelError>()
        {
            retained_actual |= std::ptr::eq(original, actual.as_ref());
        }
        source = current.source();
    }
    assert!(
        retained_actual,
        "returned fault archive retained the actual joined error Arc"
    );
    let old = Arc::downgrade(&epoch);
    assert_eq!(network.originals.snapshot().charged, 0);
    network.prepare_request_owned(end()).await.unwrap();
    let fresh = network.current().unwrap();
    assert!(!Arc::ptr_eq(&epoch, &fresh));
    drop(epoch);
    assert!(
        old.upgrade().is_some(),
        "returned typed failure retains the actual fault receiver"
    );
    use zero_connector::EventSinkTcpDialer;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut stream = tokio::select! {
        stream = network.connect(address.ip().to_string(), address.port()) => stream.unwrap(),
        _ = fresh.drive() => panic!("actual new generation stopped before connect"),
    };
    let mut peer = listener.accept().await.unwrap().0;
    assert_eq!(network.originals.snapshot().charged, 1);
    fresh
        .spawn(Box::pin(async move {
            stream.write_all(b"again").await.unwrap();
        }))
        .unwrap();
    let mut bytes = [0; 5];
    tokio::select! {
        read = peer.read_exact(&mut bytes) => { read.unwrap(); },
        _ = fresh.drive() => panic!("actual replacement IO Scope ended before wire"),
    }
    assert_eq!(&bytes, b"again");
    network.retire_generation(end()).await.unwrap();
    assert!(network.retirement_complete());
    assert_eq!(network.originals.snapshot().charged, 0);
    drop(fault);
    assert!(old.upgrade().is_none());
}

#[tokio::test]
async fn changed_event_limits_wait_for_original_scope_and_use_checked_resource_generation() {
    let network = super::super::super::ApplicationEventSinkTcpDialer::for_test(
        EgressInterfaceControl::default(),
    );
    let first = EpochLimits::new(4, 2, 1).unwrap();
    network.configure_limits(first).unwrap();
    network.begin_generation().unwrap();
    let old = network.current().unwrap();
    assert_eq!(old.limits, first);
    let instance = old.native.lock().await.instance();
    let next = EpochLimits::new(3, 1, 2).unwrap();
    assert_eq!(
        network.configure_limits(next).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(Arc::ptr_eq(&old, &network.current().unwrap()));
    network.retire_generation(end()).await.unwrap();
    let registration = old.native.lock().await.registration();
    assert!(registration.closed().unwrap().matches(&registration));
    assert!(registration.users_closed().unwrap().matches(&registration));
    network.configure_limits(next).unwrap();
    network.begin_generation().unwrap();
    let current = network.current().unwrap();
    let current_instance = current.native.lock().await.instance();
    assert_eq!(current_instance.component, instance.component);
    assert_eq!(
        current_instance.generation.0,
        instance.generation.0.checked_add(1).unwrap()
    );
    assert_eq!(
        current.generation, old.generation,
        "network epoch is not a resource generation"
    );
    assert_eq!(current.limits, next);
    assert!(!old
        .registrar
        .scope_identity()
        .same_scope(&current.registrar.scope_identity()));
    network.retire_generation(end()).await.unwrap();
}

#[tokio::test]
async fn event_native_global_total_and_local_capacity_keep_the_same_actual_original_until_return_and_join(
) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let network = super::super::super::ApplicationEventSinkTcpDialer::for_test(EgressInterfaceControl::default());
        network.originals.install_after_retirement(NonZeroUsize::new(1).unwrap()).unwrap();
        network.configure_limits(EpochLimits::new(4, 2, 1).unwrap()).unwrap();
        network.begin_generation().unwrap();
        let epoch = network.current().unwrap();
        let (mut stream, mut first_peer) = connected(&epoch).await;
        let original = epoch.originals.lock().unwrap()[0].clone();
        let (sent, received) = oneshot::channel();
        epoch.spawn(Box::pin(async move {
            stream.write_all(b"actual").await.unwrap();
            assert!(sent.send(stream).is_ok());
        })).unwrap();
        let mut bytes = [0; 6];
        let stream = tokio::select! {
            result = async { first_peer.read_exact(&mut bytes).await.unwrap(); received.await.unwrap() } => result,
            _ = epoch.drive() => panic!("actual IO worker ended before transfer"),
        };
        assert_eq!(&bytes, b"actual");
        assert_eq!(network.originals.snapshot().charged, 1);
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let denied = match epoch.connect(target.local_addr().unwrap(), &network.egress, end()).await {
            Ok(_) => panic!("local N=2 bypassed actual Host global N=1"), Err(error) => error,
        };
        assert!(denied.get_ref().and_then(|source| source.downcast_ref::<zero_platform_tokio::contract::ChannelError>())
            .is_some_and(|source| source.reason == zero_platform_tokio::contract::Reason::Capacity));
        assert!(original.adoption.closed().is_none());
        assert_eq!(network.originals.snapshot().charged, 1);
        drop(stream);
        epoch.reap().await.unwrap();
        assert!(original.adoption.closed().unwrap().matches(&original.adoption));
        assert!(original.adoption.closed().unwrap().had_native_acquisition());
        assert!(original.adoption.users_closed().unwrap().matches(&original.adoption));
        assert_eq!(network.originals.snapshot().charged, 0);
        assert_eq!(first_peer.read(&mut [0; 1]).await.unwrap(), 0);
        let (stream, _) = connected(&epoch).await;
        assert_eq!(network.originals.snapshot().charged, 1);
        drop(stream);
        network.retire_generation(end()).await.unwrap();
        assert!(network.retirement_complete());
        assert_eq!(network.originals.snapshot().charged, 0);
    }).await.unwrap();
}

use super::*;
#[cfg(feature = "managed-datagram-runtime")]
#[path = "tests/peer_binding.rs"]
mod peer_binding;
#[cfg(feature = "managed-datagram-runtime")]
#[path = "tests/peer_resource.rs"]
mod peer_resource;
#[cfg(feature = "managed-datagram-runtime")]
#[path = "tests/peers.rs"]
mod peers;
use crate::runtime::Proxy;
use std::task::Poll;
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::Notify,
    time::{timeout, Duration},
};

#[tokio::test]
async fn cancelling_the_outer_observer_retains_actual_listener_and_udp_drivers_until_host_join() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = proxy.inbound_owners.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = listener.local_addr().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_addr = socket.local_addr().unwrap();
    let (stop, mut stopping) = watch::channel(false);
    let installed = registry
        .install("fixture", &proxy.protocols, 1, stop)
        .unwrap();
    let owner = installed.owner();
    let accepting = Arc::new(Notify::new());
    let accept_entered = accepting.clone();
    owner
        .spawn_driver(async move {
            let accept = listener.accept();
            tokio::pin!(accept);
            tokio::select! {
                _ = stopping.wait_for(|stop| *stop) => Ok(()),
                _ = std::future::poll_fn(|cx| match accept.as_mut().poll(cx) {
                    Poll::Pending => { accept_entered.notify_one(); Poll::Pending },
                    Poll::Ready(result) => Poll::Ready(result),
                }) => Err(std::io::Error::other("fixture must remain at actual accept").into()),
            }
        })
        .unwrap();
    let receiving = Arc::new(Notify::new());
    let recv_entered = receiving.clone();
    let cancel = installed.tasks().cancellation();
    let udp = installed.tasks().spawn(async move {
        let mut buffer = [0; 1];
        let recv = socket.recv_from(&mut buffer);
        tokio::pin!(recv);
        tokio::select! {
            _ = cancel.cancelled() => Ok(()),
            _ = std::future::poll_fn(|cx| match recv.as_mut().poll(cx) {
                Poll::Pending => { recv_entered.notify_one(); Poll::Pending },
                Poll::Ready(result) => Poll::Ready(result),
            }) => Err(std::io::Error::other("fixture must remain at actual UDP receive").into()),
        }
    }).unwrap();
    timeout(Duration::from_secs(2), accepting.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), receiving.notified())
        .await
        .unwrap();
    let observing = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.observe_driver().await })
    };
    observing.abort();
    assert!(observing.await.unwrap_err().is_cancelled());
    assert!(TcpListener::bind(tcp_addr).await.is_err());
    assert!(UdpSocket::bind(udp_addr).await.is_err());
    assert!(registry.owner("fixture").is_some());
    registry
        .shutdown("fixture", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(registry.owner("fixture").is_none());
    let report = owner.report.lock().unwrap();
    assert!(report.exited_tasks.contains(owner.driver.get().unwrap()));
    assert!(report.exited_tasks.contains(&udp));
    assert!(report.failed_tasks.is_empty());
    drop(report);
    let _tcp = TcpListener::bind(tcp_addr).await.unwrap();
    let _udp = UdpSocket::bind(udp_addr).await.unwrap();
}

#[tokio::test]
async fn cancelled_observation_preserves_actual_task_failure_source_for_cleanup() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = proxy.inbound_owners.clone();
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("panic", &proxy.protocols, 0, stop)
        .unwrap();
    let owner = installed.owner();
    owner
        .spawn_driver(async { panic!("actual owned listener driver panic") })
        .unwrap();
    let held = owner.scope.lock().await;
    let mut observation = Box::pin(owner.observe_driver());
    std::future::poll_fn(|cx| {
        assert!(observation.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(observation);
    drop(held);
    assert!(timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .is_err());
    let report = owner.report.lock().unwrap();
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, *owner.driver.get().unwrap());
    assert!(std::error::Error::source(&report.failed_tasks[0].error).is_some());
    drop(report);
    assert!(registry
        .shutdown("panic", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert!(
        registry.owner("panic").is_some(),
        "fault/source stay with the actual owner"
    );
}

#[tokio::test]
async fn publishing_a_driver_id_after_its_real_scope_exit_preserves_the_join_receipt() {
    let (stop, _) = watch::channel(false);
    let owner = InboundOwner::new(
        ComponentInstanceId {
            component: zero_core::component::ComponentId(1),
            generation: zero_core::component::ResourceGeneration(1),
        },
        NonZeroUsize::new(1).unwrap(),
        stop,
    );
    owner.scope.lock().await.enable_registration().unwrap();
    let actual = owner
        .registrar
        .spawn(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    let exit = owner.scope.lock().await.next_exit().await.unwrap();
    assert_eq!(exit.id, actual);
    owner.record(exit);
    assert!(!owner.driver_finished.load(Ordering::Acquire));
    owner.publish_driver(actual);
    assert!(owner.driver_finished.load(Ordering::Acquire));
    timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .unwrap();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(owner.report.lock().unwrap().exited_tasks, vec![actual]);
}

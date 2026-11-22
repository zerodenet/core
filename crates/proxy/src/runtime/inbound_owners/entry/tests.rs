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
#[cfg(feature = "managed-datagram-runtime")]
#[path = "tests/responses.rs"]
mod responses;
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
    {
        let report = owner.report.lock().unwrap();
        assert!(report.exited_tasks.contains(owner.driver.get().unwrap()));
        assert!(report.exited_tasks.contains(&udp));
        assert!(report.failed_tasks.is_empty());
    }
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
    {
        let report = owner.report.lock().unwrap();
        assert_eq!(report.failed_tasks.len(), 1);
        assert_eq!(report.failed_tasks[0].id, *owner.driver.get().unwrap());
        assert!(std::error::Error::source(&report.failed_tasks[0].error).is_some());
    }
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
        Arc::default(),
    )
    .unwrap();
    owner.scope.lock().await.enable_registration().unwrap();
    let (actual, _) = owner
        .main_launch
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .spawn_tracked(async { Ok(()) })
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

#[cfg(feature = "trojan")]
mod trojan;

#[cfg(feature = "vmess")]
mod vmess;

#[cfg(feature = "shadowsocks")]
mod shadowsocks;

#[cfg(feature = "mieru")]
mod mieru;

#[cfg(any(feature = "mieru", feature = "shadowsocks", feature = "vmess"))]
mod direct_tcp {
    use super::Proxy;
    use zero_platform_tokio::controlled_carrier::{
        CarrierNativeKind, CarrierOriginObservation, CarrierOwnerRegistration,
    };

    pub(super) struct Original {
        owner: CarrierOwnerRegistration,
        native: CarrierOriginObservation,
    }

    impl Original {
        pub(super) fn capture(proxy: &Proxy) -> Self {
            let services = proxy.tcp_runtime_services();
            let slot = services.components.as_ref().unwrap().direct(None).unwrap();
            let owner = slot.original_carrier_owner(0).expect("actual Direct owner");
            let observed = owner.observe().expect("live Direct owner");
            let [stream] = observed.slots() else {
                panic!("one actual Direct original for this TCP flow");
            };
            assert_eq!(stream.native().unwrap().kind(), CarrierNativeKind::Stream);
            assert!(stream.physical_view_live() && stream.user_view_live());
            let native = stream.original().clone();
            assert!(native.closed().is_none());
            Self { owner, native }
        }

        pub(super) fn assert_closed(&self) {
            let closed = self.native.closed().expect("same Direct original closed");
            assert!(self.native.matches_closed(&closed) && closed.had_native_acquisition());
            assert!(self.owner.observe().is_none_or(|owner| {
                owner.slots().iter().all(|slot| {
                    !slot.original().same_original(&self.native)
                        || (!slot.physical_view_live() && !slot.user_view_live())
                })
            }));
        }
    }
}

#[cfg(any(feature = "trojan", feature = "vmess"))]
fn assert_tls_reads_closed(owner: &InboundOwner, cert: &std::path::Path, key: &std::path::Path) {
    let report = owner.report.lock().unwrap();
    for path in [cert, key] {
        let native = format!(
            "1 original file handles explicitly closed for {}",
            path.display()
        );
        let joined = format!(
            "1 original blocking file jobs joined for {}",
            path.display()
        );
        assert!(report.closed_io.contains(&native), "missing {native}");
        assert!(
            report.released_resources.contains(&joined),
            "missing {joined}"
        );
    }
}

#[cfg(feature = "raw-ip-runtime")]
#[path = "tests/system_tun_budget.rs"]
mod system_tun_budget;

#[cfg(feature = "raw-ip-runtime")]
#[path = "tests/source_exit.rs"]
mod source_exit;

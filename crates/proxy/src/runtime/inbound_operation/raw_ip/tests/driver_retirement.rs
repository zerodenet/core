use super::*;
use std::{future::Future, task::Poll};
use tokio::{sync::oneshot, time};
use zero_platform_tokio::contract::scope::TaskClass;

#[tokio::test]
async fn stopping_source_consumes_its_cancelled_dispatchers_successful_join() {
    time::timeout(Duration::from_secs(5), async {
        let config =
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        let registry = proxy.inbound_owners.clone();
        let (shutdown, _watch) = watch::channel(false);
        let installed = registry
            .install("packet-driver", &proxy.protocols, 1, shutdown)
            .unwrap();
        let owner = installed.owner();
        let host = installed.tasks();
        let drivers = ScopedSourceTasks::new("actual Packet dispatcher", 1, TaskClass::Critical);
        drivers.bind(installed.source_registrar()).unwrap();
        installed.register_after_scope(drivers.resource()).unwrap();
        let (pending, pending_read) = oneshot::channel();
        let (registered, original) = oneshot::channel();
        let (packets, _returns) = mpsc::channel(1);
        let (_tcp, udp) = UserNetworkStack::new(packets, 1440).into_parts();
        let main_drivers = drivers.clone();
        owner
            .spawn_driver(async move {
                let mut tasks = main_drivers.lock().await.map_err(std::io::Error::other)?;
                tasks
                    .spawn_with_witness(move |witness| async move {
                        assert!(registered.send(witness).is_ok());
                        let receive = udp.recv_datagram_with_peer();
                        tokio::pin!(receive);
                        std::future::poll_fn(|cx| {
                            assert!(receive.as_mut().poll(cx).is_pending());
                            Poll::Ready(())
                        })
                        .await;
                        pending.send(()).unwrap();
                        receive.await;
                        Err(std::io::Error::other("the pending dispatcher was not revoked").into())
                    })
                    .map_err(std::io::Error::other)?;
                let finished = tasks.join_next().await;
                assert!(matches!(
                    finished,
                    Some(Err(SourceJoinFailure::Scope { source: None, .. }))
                ));
                let outcome =
                    driver_outcome(finished, host.cancellation().is_cancelled(), &main_drivers);
                main_drivers.finish(&mut tasks).await;
                outcome
            })
            .unwrap();
        let observed = owner.clone();
        let observer = tokio::spawn(async move { observed.observe_listener().await });
        let original = original.await.unwrap();
        pending_read.await.unwrap();
        assert!(!original.is_joined());
        owner.request_stop();
        drivers.request_abort();
        observer.await.unwrap().unwrap();
        let joined = original.wait_registration_released().await;
        assert!(original.matches_joined(&joined));
        assert!(joined.registration_released());
        joined.result().unwrap();
        let closed = registry
            .confirm_shutdown(
                "packet-driver",
                time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(closed.terminal().joined_tasks(), 2);
        assert!(closed.terminal().failure().is_none());
    })
    .await
    .unwrap();
}

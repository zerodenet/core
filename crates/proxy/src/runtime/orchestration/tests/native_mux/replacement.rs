use super::*;
use std::{future::Future, task::Poll};

#[tokio::test]
async fn native_mux_network_epoch_retires_original_and_waits_for_old_logical_io_before_acquiring() {
    timeout(Duration::from_secs(30), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let proxy = Proxy::from_engine_with_background(
                zero_engine::Engine::new(config(protocol, server.fixture.front.port())).unwrap(),
                true,
            )
            .unwrap();
            let running = proxy.spawn();
            crate::ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            )
            .apply_runtime_config_and_wait(
                proxy.engine().config().as_ref().clone(),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            let mut held = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut held).await;
            let registration = registration(&proxy);
            let observation = registration.observe().unwrap();
            let original = observation.slots()[0].original().clone();
            let driver = observation.slots()[0].driver().unwrap().1.clone();
            {
                let mut byte = [0];
                let pending = held.read(&mut byte);
                tokio::pin!(pending);
                std::future::poll_fn(|cx| {
                    assert!(
                        pending.as_mut().poll(cx).is_pending(),
                        "real MUX receive must be suspended"
                    );
                    Poll::Ready(())
                })
                .await;
                proxy.egress_interface_control().invalidate_network();
                let reopening = connect(&proxy, server.fixture.echo.port());
                tokio::pin!(reopening);
                std::future::poll_fn(|cx| {
                    assert!(
                        reopening.as_mut().poll(cx).is_pending(),
                        "retained old user must keep new acquisition pending"
                    );
                    if original.closed().is_some() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                assert!(driver.is_joined());
                let proof = original
                    .closed()
                    .expect("actual original native controller closes before another acquisition");
                assert!(original.matches_closed(&proof) && proof.had_native_acquisition());
                let result = timeout(Duration::from_secs(1), &mut pending).await.unwrap();
                assert!(
                    matches!(result, Ok(0) | Err(_)),
                    "retired MUX I/O cannot stay live"
                );
                assert_eq!(server.fixture.connections(), 1);
            }
            drop(held);
            let mut fresh = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut fresh).await;
            assert_eq!(server.fixture.connections(), 2);
            let next = registration.observe().unwrap();
            assert_eq!(next.slots().len(), 1);
            assert!(!next.slots()[0].original().same_original(&original));
            assert!(!driver.same_registration(&next.slots()[0].driver().unwrap().1));
            drop(fresh);
            running.shutdown().await.unwrap();
            server.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_mux_real_apply_waits_for_prior_original_users_and_keeps_historical_role_revoked() {
    timeout(Duration::from_secs(40), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let config = config(protocol, server.fixture.front.port());
            let proxy = Proxy::from_engine_with_background(
                zero_engine::Engine::new(config.clone()).unwrap(),
                true,
            )
            .unwrap();
            let running = proxy.spawn();
            let handle = crate::ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
                .await
                .unwrap();
            let mut held = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut held).await;
            let registration = registration(&proxy);
            let observation = registration.observe().unwrap();
            let original = observation.slots()[0].original().clone();
            let driver = observation.slots()[0].driver().unwrap().1.clone();
            let old_plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let old_slot = old_plan.configured("a").unwrap();
            let mut candidate = config;
            match &mut candidate.outbounds[0].protocol {
                zero_config::OutboundProtocolConfig::Vless {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Vmess {
                    mux_concurrency, ..
                }
                | zero_config::OutboundProtocolConfig::Trojan {
                    mux_concurrency, ..
                } => *mux_concurrency = Some(3),
                _ => unreachable!(),
            }
            let replacing = tokio::spawn(async move {
                handle
                    .apply_runtime_config_and_wait(candidate, Duration::from_secs(10))
                    .await
            });
            timeout(Duration::from_secs(3), driver.wait())
                .await
                .unwrap();
            assert!(
                !replacing.is_finished(),
                "actual held logical I/O must block replacement completion"
            );
            assert_eq!(server.fixture.connections(), 1);

            assert!(old_slot.stream(deadline()).await.is_err());
            drop(held);
            replacing.await.unwrap().unwrap();
            assert!(registration.closed().unwrap().matches(&registration));
            assert!(registration.users_closed().unwrap().matches(&registration));
            assert!(original.matches_closed(&original.closed().unwrap()));
            let next_plan = proxy.tcp_runtime_services().components.clone().unwrap();
            assert_ne!(
                old_slot.instance_id(),
                next_plan.configured("a").unwrap().instance_id()
            );
            assert!(
                old_slot.stream(deadline()).await.is_err(),
                "historical role cannot bind the successor pool"
            );
            let mut fresh = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut fresh).await;
            assert_eq!(server.fixture.connections(), 2);
            drop(fresh);
            running.shutdown().await.unwrap();
            server.close().await;
        }
    })
    .await
    .unwrap();
}

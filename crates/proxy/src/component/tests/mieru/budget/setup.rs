//! The ordinary owning Scope must reuse its one real task credit after setup.
use super::*;

#[tokio::test]
async fn ordinary_one_carrier_preparation_and_driver_share_one_scope_credit_for_both_underlays() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for udp in [false, true] {
            let server = if udp {
                Server::packet(false).await
            } else {
                Server::new(false).await
            };
            let mut config = config([server.address; 3]);
            config.runtime.mieru_max_carriers = Some(1);
            if udp {
                for node in &mut config.outbounds {
                    let zero_config::OutboundProtocolConfig::Mieru { transport, .. } =
                        &mut node.protocol
                    else {
                        unreachable!()
                    };
                    *transport = zero_config::MieruTransport::Udp;
                }
            }
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            )
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
            assert_eq!(server.accepts.load(Ordering::SeqCst), 0);
            let mut first = open(&proxy, "a").await.unwrap();
            echo(&mut first).await;
            let original = first.owner.original().clone();
            assert_eq!(original.owner().limit().get(), 1);
            let driver = original
                .driver()
                .expect("same actual registered driver")
                .clone();
            let mut second = open(&proxy, "a").await.unwrap();
            echo(&mut second).await;
            assert!(second.owner.original().same_original(&original));
            returned(first).await;
            assert!(!driver.is_joined());
            assert!(original.closed().is_none());
            echo(&mut second).await;
            let mut third = open(&proxy, "a").await.unwrap();
            echo(&mut third).await;
            assert!(third.owner.original().same_original(&original));
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
            returned(second).await;
            returned(third).await;
            running.shutdown().await.unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            let native = original.closed().expect("actual original native closure");
            assert!(native.matches(&original) && native.had_native_acquisition());
            assert!(original.owner().closed().unwrap().matches(original.owner()));
            server.close().await;
        }
    })
    .await
    .unwrap();
}

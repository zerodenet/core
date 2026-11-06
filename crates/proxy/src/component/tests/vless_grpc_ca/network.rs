//! Network replacement retains the same configuration's authorized TLS material.
use super::*;
#[tokio::test]
async fn ordinary_same_config_network_replacement_uses_original_ca_after_its_path_is_removed() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for backend in ["rustls", "openssl"] {
            let directory = Directory::new();
            let path = directory.0.join("ca.pem");
            let peer = Peer::new().await;
            std::fs::write(&path, &peer.pem).unwrap();
            let config = config(&peer, &path, backend, true);
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config, Duration::from_secs(5))
                .await
                .unwrap();
            let snapshot = proxy.engine().runtime_snapshot().config().clone();
            let mut first = loan(&proxy).await;
            echo(&mut first).await;
            let prior = first.owner.original().clone();
            returned(first).await;
            std::fs::remove_file(&path).unwrap();
            proxy.egress_interface_control().invalidate_network();
            let mut fresh = loan(&proxy).await;
            echo(&mut fresh).await;
            let current = fresh.owner.original().clone();
            assert!(Arc::ptr_eq(
                proxy.engine().runtime_snapshot().config(),
                &snapshot
            ));
            assert!(!current.same_original(&prior));
            assert_eq!(current.instance().component, prior.instance().component);
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 2);
            let old_driver = prior.driver().unwrap();
            let joined = old_driver.wait_registration_released().await;
            assert!(old_driver.matches_joined(&joined) && joined.registration_released());
            assert!(prior.closed().unwrap().matches(&prior));
            returned(fresh).await;
            running.shutdown().await.unwrap();
            let driver = current.driver().unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            assert!(current.closed().unwrap().matches(&current));
            assert!(current.owner().closed().unwrap().matches(current.owner()));
            peer.close().await;
        }
    })
    .await
    .unwrap();
}

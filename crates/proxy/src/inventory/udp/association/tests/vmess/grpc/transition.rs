//! Actual legacy/native admission never treats removal of a recipe as closure.
use super::*;
#[path = "transition/fixtures.rs"]
mod fixtures;
use fixtures::{config, echo, open, Peer};
#[tokio::test]
async fn ordinary_vmess_grpc_legacy_to_native_reload_rejects_before_stopping_old_tcp_or_opening_new_originals(
) {
    timeout(Duration::from_secs(20), async {
        let peer = Peer::new().await;
        let initial = config(&peer, true);
        let (proxy, running, handle) = fixtures::start(initial.clone()).await;
        let mut old = open(&proxy, "node").await;
        echo(&mut old).await;
        let slot = proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("node")
            .unwrap();
        assert!(!slot
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Stream));
        assert_eq!(
            original(&proxy).observe().unwrap().retained(),
            0,
            "legacy TCP is not in Datagram native users"
        );
        let revision = proxy.config_revision();
        let promoted = config(&peer, false);
        let error = handle
            .apply_runtime_config_and_wait(promoted.clone(), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(error.contains("legacy TCP originals"), "{error}");
        assert_eq!(proxy.config_revision(), revision);
        assert_eq!(
            proxy.engine().runtime_snapshot().config().as_ref(),
            &initial
        );
        assert_eq!(
            slot.instance_id(),
            proxy
                .tcp_runtime_services()
                .components
                .as_ref()
                .unwrap()
                .configured("node")
                .unwrap()
                .instance_id()
        );
        echo(&mut old).await;
        assert_eq!(peer.count(), 1);
        drop(old);
        // There is no actual legacy native/driver receipt. Drop cannot clear
        // the exposure debt or authorize a new executor.
        assert!(handle
            .apply_runtime_config_and_wait(promoted, Duration::from_secs(5))
            .await
            .unwrap_err()
            .contains("legacy TCP originals"));
        let mut retained = open(&proxy, "node").await;
        echo(&mut retained).await;
        assert_eq!(peer.count(), 2);
        drop(retained);
        running.shutdown().await.unwrap();
        peer.close().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn ordinary_vmess_grpc_native_to_legacy_reload_preserves_tcp_until_explicit_removal_confirms_original_close(
) {
    timeout(Duration::from_secs(25), async {
        let peer = Peer::new().await;
        let initial = config(&peer, false);
        let (proxy, running, handle) = fixtures::start(initial.clone()).await;
        let mut old = open(&proxy, "node").await;
        echo(&mut old).await;
        let slot = proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("node")
            .unwrap();
        let registration = slot.original_carrier_owner(1).unwrap();
        let observed = registration.observe().unwrap();
        let original = observed.slots()[0].original().clone();
        let driver = observed.slots()[0].driver().unwrap().1.clone();
        let error = handle
            .apply_runtime_config_and_wait(config(&peer, true), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(error.contains("unsupported legacy grammar"), "{error}");
        assert_eq!(
            proxy.engine().runtime_snapshot().config().as_ref(),
            &initial
        );
        assert!(original.closed().is_none());
        assert!(!driver.is_joined());
        echo(&mut old).await;
        assert_eq!(peer.count(), 1);
        let empty =
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
        let applying = handle.clone();
        let removing = tokio::spawn(async move {
            applying
                .apply_runtime_config_and_wait(empty, Duration::from_secs(10))
                .await
        });
        timeout(Duration::from_secs(3), driver.wait())
            .await
            .unwrap();
        assert!(
            !removing.is_finished(),
            "returned driver cannot replace actual held TCP view"
        );
        drop(old);
        removing.await.unwrap().unwrap();
        assert!(original.matches_closed(&original.closed().unwrap()));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        // Only that positive original cleanup permits a later legacy candidate.
        handle
            .apply_runtime_config_and_wait(config(&peer, true), Duration::from_secs(5))
            .await
            .unwrap();
        let mut next = open(&proxy, "node").await;
        echo(&mut next).await;
        assert_eq!(peer.count(), 2);
        drop(next);
        running.shutdown().await.unwrap();
        peer.close().await;
    })
    .await
    .unwrap();
}

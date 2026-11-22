//! Changes to Stream eligibility keep both the actual role and original-close gates.
use super::*;
#[path = "transition/fixtures.rs"]
mod fixtures;
use fixtures::{config, echo, open, Peer};
#[tokio::test]
async fn ordinary_vmess_grpc_datagram_only_to_native_stream_waits_for_live_udp_original_return() {
    timeout(Duration::from_secs(20), async {
        let peer = Peer::new().await;
        let initial = config(&peer, true);
        let (proxy, running, handle) = fixtures::start(initial).await;
        assert_standard_datagram(&proxy);
        fixtures::assert_tcp_unavailable(&proxy).await;
        assert_eq!(peer.count(), 0);

        let registration = original(&proxy);
        let flow = flow::open(proxy.clone(), 1, b"live").await;
        let observed = registration.observe().unwrap();
        let native = observed.slots()[0].original().clone();
        let driver = observed.slots()[0].driver().unwrap().1.clone();
        let promoted = config(&peer, false);
        let applying = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(promoted, Duration::from_secs(10))
                .await
        });
        timeout(Duration::from_secs(3), driver.wait())
            .await
            .unwrap();
        assert!(!applying.is_finished());
        assert!(registration.closed().is_none());
        assert_eq!(peer.count(), 1);

        flow.finish_all().await;
        applying.await.unwrap().unwrap();
        assert!(native.matches_closed(&native.closed().unwrap()));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert_ne!(original(&proxy).instance(), registration.instance());

        let mut tcp = open(&proxy, "node").await;
        echo(&mut tcp).await;
        assert_eq!(peer.count(), 2);
        drop(tcp);
        running.shutdown().await.unwrap();
        peer.close().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn ordinary_vmess_grpc_native_to_datagram_only_reload_preserves_tcp_until_original_close() {
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
        // Only confirmed original cleanup permits installing the candidate
        // without Stream. Its actual Datagram role can still serve traffic.
        handle
            .apply_runtime_config_and_wait(config(&peer, true), Duration::from_secs(5))
            .await
            .unwrap();
        fixtures::assert_tcp_unavailable(&proxy).await;
        assert_eq!(peer.count(), 1);
        let flow = flow::open(proxy.clone(), 2, b"live").await;
        assert_eq!(peer.count(), 2);
        flow.finish_all().await;
        running.shutdown().await.unwrap();
        peer.close().await;
    })
    .await
    .unwrap();
}

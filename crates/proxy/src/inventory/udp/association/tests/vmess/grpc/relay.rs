//! Actual ordinary catalog relay selection, wire, accounting and original custody.
use super::*;
#[path = "relay/fixtures.rs"]
mod fixtures;
use fixtures::*;
#[cfg(all(feature = "vless", feature = "trojan"))]
#[path = "relay/loans.rs"]
mod loans;
#[path = "relay/network.rs"]
mod network;
#[cfg(all(feature = "vless", feature = "trojan"))]
#[path = "relay/pool_fixture.rs"]
mod pool_fixture;
#[cfg(all(feature = "vless", feature = "trojan"))]
#[path = "relay/pooled.rs"]
mod pooled;
#[cfg(all(feature = "vless", feature = "trojan"))]
#[path = "relay/production.rs"]
mod production;
#[path = "relay/replacement.rs"]
mod replacement;
#[tokio::test]
async fn ordinary_vmess_grpc_relay_datagram_preserves_stream_prefixes_tls_modes_payloads_and_accounting(
) {
    timeout(Duration::from_secs(30), async {
        for (tls, multi, cipher, kind, two) in [
            (false, false, "aes-128-gcm", Kind::Vmess, false),
            (true, false, "chacha20-poly1305", Kind::Socks, true),
            (true, true, "zero", Kind::Vmess, true),
            (false, true, "aes-128-gcm", Kind::Socks, false),
        ] {
            let first = Prefix::new(kind, false).await;
            let second = Prefix::new(Kind::Socks, false).await;
            let prefixes = if two {
                vec![first.outbound("first"), second.outbound("second")]
            } else {
                vec![first.outbound("first")]
            };
            let payloads = vec![b"relay".to_vec(), vec![], vec![9; 37000]];
            let (proxy, server) = start_config(
                tls,
                multi,
                cipher,
                "node",
                payloads.clone(),
                1,
                tls,
                |config| configure(config, &prefixes, two),
            )
            .await;
            assert_standard_relay(&proxy);
            proxy
                .protocols
                .drive_component_supervision(async {
                    let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("incoming")
                        .await
                        .unwrap();
                    let mut session_id = None;
                    for payload in &payloads {
                        let id = dispatch.dispatch(input(53, payload)).await.unwrap();
                        if let Some(old) = session_id {
                            assert_eq!(id, old);
                        }
                        session_id = Some(id);
                        let packet = response(&mut dispatch).await;
                        assert_eq!(&packet.2, payload);
                        crate::runtime::udp_delivery::record_chain_udp_response_parts(
                            &proxy.udp_runtime_services(),
                            &dispatch,
                            packet.0,
                            packet.1,
                            packet.2,
                            packet.3,
                        );
                    }
                    let sessions = proxy.engine().active_sessions();
                    let session = sessions
                        .iter()
                        .find(|s| s.id == session_id.unwrap())
                        .unwrap();
                    assert_eq!(session.outbound_tag.as_deref(), Some("node"));
                    assert_eq!(session.outbound_tx_bytes, 37005 + 27);
                    assert_eq!(session.outbound_rx_bytes, 37005);
                    assert_eq!(
                        session.path.remote.as_ref().unwrap().port,
                        first.address.port()
                    );
                    assert_eq!(first.count.load(std::sync::atomic::Ordering::SeqCst), 1);
                    assert_eq!(
                        second.count.load(std::sync::atomic::Ordering::SeqCst),
                        usize::from(two)
                    );
                    let registration = original(&proxy);
                    let observed = registration.observe().unwrap();
                    assert_eq!(observed.retained(), 1);
                    let native = observed.slots()[0].original().clone();
                    let driver = observed.slots()[0].driver().unwrap().1.clone();
                    close_flow(&proxy, dispatch).await;
                    assert!(driver.is_joined());
                    assert!(native.matches_closed(&native.closed().unwrap()));
                    server.finish().await;
                    proxy
                        .protocols
                        .shutdown_components(Instant::now() + Duration::from_secs(3))
                        .await
                        .unwrap();
                    assert!(registration.closed().unwrap().matches(&registration));
                })
                .await;
            first.close().await;
            second.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_relay_rejected_prefix_never_bypasses_to_final_or_legacy_udp() {
    timeout(Duration::from_secs(12), async {
        let mut prefix = Prefix::new(Kind::Reject, true).await;
        let (proxy, server) = start_config(
            false,
            false,
            "aes-128-gcm",
            "node",
            vec![],
            1,
            false,
            |config| configure(config, &[prefix.outbound("first")], false),
        )
        .await;
        assert_standard_relay(&proxy);
        proxy
            .protocols
            .drive_component_supervision(async {
                let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("incoming")
                    .await
                    .unwrap();
                let opening=dispatch.dispatch(input(53,b"must-not-send"));
                tokio::pin!(opening);
                tokio::select! { biased;
                    result=&mut opening=>panic!("actual rejected CONNECT still awaits peer response: {result:?}"),
                    _=prefix.request()=>{}
                }
                let registration=original(&proxy);
                let waiting=registration.observe().unwrap();assert_eq!(waiting.retained(),1);
                assert!(waiting.slots()[0].native().is_none());assert!(waiting.slots()[0].driver().is_none());
                let consumer=waiting.slots()[0].original().clone();
                let donor=donor_owner(&proxy,"first");let waiting=donor.observe().unwrap();
                assert_eq!(waiting.retained(),1);assert!(waiting.slots()[0].native().is_some());
                assert!(waiting.slots()[0].physical_view_live());
                let native=waiting.slots()[0].original().clone();
                prefix.release_response();
                assert!(opening.await.is_err());
                assert!(consumer.matches_closed(&consumer.closed().unwrap()));
                assert!(!consumer.closed().unwrap().had_native_acquisition());
                assert_eq!(prefix.count.load(std::sync::atomic::Ordering::SeqCst), 1);
                assert_eq!(
                    server.accepted(),
                    0,
                    "the bound final listener must receive no direct connection"
                );
                assert_eq!(registration.observe().unwrap().retained(),0,"rejected unused consumer is reclaimed");
                dispatch.finish_all();
                proxy
                    .protocols
                    .shutdown_components(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
                assert!(native.matches_closed(&native.closed().unwrap()));
                assert!(native.closed().unwrap().had_native_acquisition());
                assert!(donor.closed().unwrap().matches(&donor));
                assert!(donor.users_closed().unwrap().matches(&donor));
                assert!(registration.closed().unwrap().matches(&registration));
                server.finish().await;
            })
            .await;
        prefix.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_relay_revoked_prefix_cannot_reopen_through_legacy_or_direct() {
    timeout(Duration::from_secs(12), async {
        let prefix = Prefix::new(Kind::Socks, false).await;
        let (proxy, server) = start_config(
            false,
            false,
            "aes-128-gcm",
            "node",
            vec![],
            1,
            false,
            |config| configure(config, &[prefix.outbound("first")], false),
        )
        .await;
        assert_standard_relay(&proxy);
        proxy
            .protocols
            .drive_component_supervision(async {
                let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                let stopped = plan.configured("first").unwrap();
                assert!(stopped
                    .shutdown(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap()
                    .is_complete());
                let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("incoming")
                    .await
                    .unwrap();
                let opening=dispatch.dispatch(input(53,b"must-not-send"));
                tokio::pin!(opening);
                tokio::select! { biased;
                    result=&mut opening=>panic!("actual rejected CONNECT still awaits peer response: {result:?}"),
                    _=prefix.request()=>{}
                }
                let registration=original(&proxy);
                let waiting=registration.observe().unwrap();assert_eq!(waiting.retained(),1);
                assert!(waiting.slots()[0].native().is_none());assert!(waiting.slots()[0].driver().is_none());
                let consumer=waiting.slots()[0].original().clone();
                let donor=donor_owner(&proxy,"first");let waiting=donor.observe().unwrap();
                assert_eq!(waiting.retained(),1);assert!(waiting.slots()[0].native().is_some());
                assert!(waiting.slots()[0].physical_view_live());
                let native=waiting.slots()[0].original().clone();
                prefix.release_response();
                assert!(opening.await.is_err());
                assert!(consumer.matches_closed(&consumer.closed().unwrap()));
                assert!(!consumer.closed().unwrap().had_native_acquisition());
                assert_eq!(prefix.count.load(std::sync::atomic::Ordering::SeqCst), 0);
                assert_eq!(server.accepted(), 0);
                assert_eq!(
                    original(&proxy).observe().unwrap().retained(),
                    0,
                    "reject before acquiring an original"
                );
                dispatch.finish_all();
                proxy
                    .protocols
                    .shutdown_components(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
                server.finish().await;
            })
            .await;
        prefix.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_relay_udp_policy_rejects_before_any_prefix_or_final_io() {
    timeout(Duration::from_secs(12), async {
        let prefix = Prefix::new(Kind::Socks, false).await;
        let mut node = prefix.outbound("first");
        node.udp.enabled = false;
        let (proxy, server) = start_config(
            false,
            false,
            "aes-128-gcm",
            "node",
            vec![],
            1,
            false,
            |config| configure(config, &[node], false),
        )
        .await;
        proxy
            .protocols
            .drive_component_supervision(async {
                let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("incoming")
                    .await
                    .unwrap();
                let opening=dispatch.dispatch(input(53,b"must-not-send"));
                tokio::pin!(opening);
                tokio::select! { biased;
                    result=&mut opening=>panic!("actual rejected CONNECT still awaits peer response: {result:?}"),
                    _=prefix.request()=>{}
                }
                let registration=original(&proxy);
                let waiting=registration.observe().unwrap();assert_eq!(waiting.retained(),1);
                assert!(waiting.slots()[0].native().is_none());assert!(waiting.slots()[0].driver().is_none());
                let consumer=waiting.slots()[0].original().clone();
                let donor=donor_owner(&proxy,"first");let waiting=donor.observe().unwrap();
                assert_eq!(waiting.retained(),1);assert!(waiting.slots()[0].native().is_some());
                assert!(waiting.slots()[0].physical_view_live());
                let native=waiting.slots()[0].original().clone();
                prefix.release_response();
                assert!(opening.await.is_err());
                assert!(consumer.matches_closed(&consumer.closed().unwrap()));
                assert!(!consumer.closed().unwrap().had_native_acquisition());
                assert_eq!(prefix.count.load(std::sync::atomic::Ordering::SeqCst), 0);
                assert_eq!(server.accepted(), 0);
                assert_eq!(original(&proxy).observe().unwrap().retained(), 0);
                dispatch.finish_all();
                proxy
                    .protocols
                    .shutdown_components(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
                server.finish().await;
            })
            .await;
        prefix.close().await;
    })
    .await
    .unwrap();
}

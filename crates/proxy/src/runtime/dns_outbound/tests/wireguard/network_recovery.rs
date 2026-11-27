//! Network-change and failed-reload recovery of shared default/node DoH pools
//! and standalone WireGuard outer UDP bindings on a single running Proxy.
use super::*;
use std::sync::atomic::Ordering;
use zero_platform_tokio::contract::{
    dns::{DnsQueryContext, DnsQueryRole, DnsRequest},
    CancellationToken,
};

async fn answer_doh(tls: &mut crate::dns_fixture::TlsDns, ip: [u8; 4]) {
    let (bytes, reply) = tokio::time::timeout_at(end(), tls.requests.recv())
        .await
        .expect("DoH server timed out waiting for query")
        .expect("DoH request channel closed");
    reply
        .send(zero_dns::udp::build_dns_response(
            &bytes,
            &[IpAddress::V4(ip)],
        ))
        .unwrap();
}

async fn resolve_default_and_node(
    proxy: &Proxy,
    tls: &mut crate::dns_fixture::TlsDns,
    prefix: &str,
    expected: [u8; 4],
) {
    let default_task = query(proxy, &format!("{prefix}.default.wg.test"));
    answer_doh(tls, expected).await;
    assert_eq!(
        tokio::time::timeout_at(end(), default_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        vec![IpAddress::V4(expected)]
    );

    let node_service = proxy.resolver.native_node_service();
    let node_name = format!("{prefix}.node.wg.test");
    let node_task = tokio::spawn(async move {
        node_service
            .resolve(
                DnsRequest::new(&node_name, DnsQueryRole::Node),
                DnsQueryContext {
                    deadline: Some(tokio::time::Instant::now() + Duration::from_secs(3)),
                    cancel: CancellationToken::new(),
                    expected_generation: None,
                },
            )
            .await
    });
    answer_doh(tls, expected).await;
    let resolved = tokio::time::timeout_at(end(), node_task)
        .await
        .unwrap()
        .unwrap()
        .expect("node DoH query must succeed without `DoH pool is closing`");
    assert_eq!(resolved.addresses, vec![IpAddress::V4(expected)]);
}

#[tokio::test]
async fn standalone_wireguard_network_change_and_failed_reload_rebuild_shared_doh_pool_without_exiting(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut tls = crate::dns_fixture::TlsDns::new(true).await;
        let peer = peer::Peer::new(tls.address).await;
        let mut backend = tls.server(true);
        if let zero_config::DnsServerConfig::Doh { host, detour, .. } = &mut backend {
            *host = "10.0.0.2".into();
            *detour = Some("wg".into());
        }
        let document = serde_json::json!({
            "runtime": {
                "dns": {
                    "servers": {
                        "alidns": backend
                    },
                    "default_server": "alidns",
                    "policy": {
                        "node_server": "alidns",
                        "address_family": "ipv4_only",
                        "timeout_ms": 3000
                    }
                }
            },
            "outbounds": [{
                "tag": "wg",
                "protocol": {
                    "type": "wireguard",
                    "private_key": STANDARD.encode([121; 32]),
                    "addresses": ["10.0.0.1/32"],
                    "peers": [{
                        "public_key": peer::public(122),
                        "endpoint": peer.address.to_string(),
                        "allowed_ips": ["0.0.0.0/0"]
                    }]
                }
            }],
            "route": { "final": { "type": "direct" } }
        });
        let config = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "alidns", tls.certificate.clone())
            .unwrap();

        let old_root = proxy
            .protocols
            .test_packet_source_origins(proxy.engine().config().as_ref())
            .remove(0);

        // Warm both `default` and `node` queries over the same host-bound AliDNS DoH pool.
        resolve_default_and_node(&proxy, &mut tls, "before-switch", [198, 51, 100, 1]).await;
        let old_doh = pending(&proxy).await;
        assert_eq!(tls.connections.load(Ordering::SeqCst), 1);

        // 1. Trigger physical network switch (`egress` generation bump) while the standalone
        //    WireGuard device and host-bound DoH pool are live.
        let mut reconciled = proxy.reconciled_device_generations();
        let target_gen = proxy.egress_interface.generation() + 1;
        proxy.egress_interface.invalidate_network();
        tokio::time::timeout(
            Duration::from_secs(5),
            reconciled.wait_for(|g| *g >= target_gen),
        )
        .await
        .expect("reconcile_outbound_network must complete without fatal exit")
        .unwrap();

        // Old DoH driver and old WireGuard outer UDP socket must be confirmed retired.
        assert!(old_doh.task.is_joined());
        assert!(old_root.closed().unwrap().matches(&old_root));

        // Fresh DoH backend must already be published (accepting CA install) before queries.
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "alidns", tls.certificate.clone())
            .expect("network reconciliation must publish a fresh open DoH backend");

        resolve_default_and_node(&proxy, &mut tls, "after-switch", [198, 51, 100, 2]).await;

        // 2. Trigger a failed reload (candidate inbound fails port bind AFTER StopOld closed
        //    the live DoH pool) and verify `reject_reload` restores an open DoH backend.
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut bad_doc = document.clone();
        bad_doc["inbounds"] = serde_json::json!([{
            "tag": "conflict",
            "listen": { "address": "127.0.0.1", "port": occupied.local_addr().unwrap().port() },
            "protocol": { "type": "direct", "target": "127.0.0.1", "port": 9 }
        }]);
        let bad_config = zero_config::RuntimeConfig::parse(&bad_doc.to_string()).unwrap();
        let err = handle
            .apply_runtime_config_and_wait(bad_config, Duration::from_secs(5))
            .await
            .expect_err("occupied port reload must fail and roll back");
        assert!(!err.is_empty());
        drop(occupied);

        let _ = zero_dns::test_support::install_doh_ca(
            &proxy.resolver,
            "alidns",
            tls.certificate.clone(),
        );
        resolve_default_and_node(&proxy, &mut tls, "after-rollback", [198, 51, 100, 3]).await;

        let current_root = proxy
            .protocols
            .test_packet_source_origins(proxy.engine().config().as_ref())
            .remove(0);
        assert!(!current_root.same_original(&old_root));
        running.shutdown().await.unwrap();
        assert!(current_root.closed().unwrap().matches(&current_root));
        peer.close().await;
        tls.close().await;
    })
    .await
    .unwrap();
}

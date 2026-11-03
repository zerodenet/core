//! An unchanged pure profile retains its actual active generation, not just bytes.
use super::*;
use zero_platform_tokio::{contract::dns::DnsService, controlled_carrier::loan::DatagramLoan};

#[tokio::test]
async fn hysteria2_public_unchanged_reload_preserves_active_tcp_udp_and_original_authentication() {
    unchanged(false).await;
}

#[tokio::test]
async fn hysteria2_public_same_ca_bytes_reload_preserves_active_tcp_udp_and_original_authentication(
) {
    unchanged(true).await;
}

async fn unchanged(with_ca: bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        let ca = with_ca.then(|| {
            let path = server._material.path().join("unchanged-client-ca.pem");
            std::fs::copy(server._material.path().join("cert.pem"), &path).unwrap();
            path
        });
        let mut configured_dns = if with_ca {
            Some(crate::dns_fixture::Dns::new().await)
        } else {
            None
        };
        let mut unmatched_dns = if with_ca {Some(crate::dns_fixture::Dns::new().await)} else {None};
        let mut config = reload::config(&server, ca.as_deref());
        if let Some(dns) = &configured_dns {
            let directory=server._material.path();
            std::fs::write(directory.join("node-domains.txt"),b"unchanged.node.test\n").unwrap();
            config.source_dir=Some(directory.to_owned());
            config.route.rule_sets=serde_json::from_value(serde_json::json!([
                {"tag":"node-domains","type":"file","path":"node-domains.txt","format":"domain_list"}
            ])).unwrap();
            let mut policy=dns.config();
            policy.servers.insert("unmatched".into(),unmatched_dns.as_ref().unwrap().server());
            policy.default_server="unmatched".into();
            policy.dispatch=serde_json::from_value(serde_json::json!([
                {"condition":{"type":"rule_set","tag":"node-domains"},"server":"native"}
            ])).unwrap();
            config.runtime.dns = Some(policy);
            let zero_config::OutboundProtocolConfig::Hysteria2 { server, .. } =
                &mut config.outbounds[0].protocol
            else {
                unreachable!()
            };
            *server = "unchanged.node.test".into();
        }
        let (proxy, running, handle) = reload::running(config.clone()).await;
        let mut tcp = if let Some(dns) = &mut configured_dns {
            let mut connecting = Box::pin(connect(&proxy, "hy"));
            let request = tokio::select! {
                request = dns.request() => request,
                _ = unmatched_dns.as_mut().unwrap().request() => panic!("Node DNS ignored the actual file-backed dispatcher"),
                _ = &mut connecting => panic!("the actual configured Node DNS was bypassed"),
            };
            request.answer(false, false);
            connecting.await
        } else {
            connect(&proxy, "hy").await
        };
        let udp = reload::loan(&proxy, 930).await;
        let root = udp.owner.original().clone();
        let original = reload::original(&root);
        let revision = proxy.engine().config_revision();
        let dns = proxy.resolver.generation();
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .expect(
                "unchanged pure profiles must acknowledge without returning active TCP/UDP users",
            );
        assert_eq!(proxy.engine().config_revision(), revision + 1);
        assert_eq!(
            proxy.resolver.generation(),
            dns,
            "unchanged Node DNS stays the same actual authority"
        );
        assert!(original.closed().is_none());
        assert!(root.closed().is_none() && !root.is_retired());
        tcp.write_all(b"ping").await.unwrap();
        let mut response = [0; 4];
        tcp.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &response, b"ping",
            "the same actual TCP stream still forwards"
        );
        udp.io.send(b"after unchanged reload").await.unwrap();
        let mut response = [0; 32];
        let count = udp.io.receive(&mut response).await.unwrap();
        assert_eq!(&response[..count], b"after unchanged reload");
        let fresh = reload::loan(&proxy, 931).await;
        assert_eq!(fresh.owner.original().instance(), root.instance());
        assert!(reload::original(fresh.owner.original()).same_original(&original));
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        assert_eq!(server.closed.load(Ordering::SeqCst), 0);
        if let Some(dns) = &mut configured_dns {
            dns.no_new_request();
            unmatched_dns.as_mut().unwrap().no_new_request();
            assert_eq!(std::fs::read(server._material.path().join("node-domains.txt")).unwrap(),b"unchanged.node.test\n");
            assert_eq!(proxy.engine().runtime_snapshot().dns_dispatch().unwrap().select("unchanged.node.test"),"native");
        }
        for channel in [udp, fresh] {
            let DatagramLoan { io, mut owner } = channel;
            drop(io);
            assert!(owner.close(end()).await.unwrap().is_complete());
            drop(owner);
        }
        drop(tcp);
        running.shutdown().await.unwrap();
        assert!(original.closed().unwrap().had_native_acquisition());
        server.close().await;
        if let Some(dns) = configured_dns {
            dns.close().await;
        }
        if let Some(dns)=unmatched_dns {dns.close().await;}
    })
    .await
    .unwrap();
}

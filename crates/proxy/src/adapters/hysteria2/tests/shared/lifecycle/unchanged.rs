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
        let mut config = reload::config(&server, ca.as_deref());
        if let Some(dns) = &configured_dns {
            config.runtime.dns = Some(dns.config());
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
    })
    .await
    .unwrap();
}

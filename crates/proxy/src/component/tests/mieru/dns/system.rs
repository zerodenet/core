//! Ordinary assembly, not just the background-qualified protocol fixture.
use super::*;
use zero_platform_tokio::contract::dns::{DnsQueryContext, DnsQueryRole, DnsRequest};

#[tokio::test]
async fn ordinary_proxy_default_system_uses_its_actual_roleless_dns_host() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let config = config(["127.0.0.1:1".parse().unwrap(); 3]);
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
        let service = proxy.resolver.native_node_service();
        let generation = service.generation();
        let response = service
            .resolve(
                DnsRequest {
                    name: "localhost".into(),
                    role: DnsQueryRole::Node,
                    family: zero_traits::AddressFamily::OnlyIpv4,
                },
                DnsQueryContext {
                    deadline: Some(deadline()),
                    cancel: CancellationToken::new(),
                    expected_generation: Some(generation),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response.addresses,
            vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]
        );
        assert_eq!(response.generation, generation);
        let mut stale = DnsQueryContext {
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            expected_generation: Some(generation),
        };
        proxy.egress_interface_control().invalidate_network();
        let request = DnsRequest {
            name: "localhost".into(),
            role: DnsQueryRole::Node,
            family: Default::default(),
        };
        assert!(service
            .resolve(request.clone(), stale.clone())
            .await
            .is_err());
        stale.expected_generation = None;
        service
            .resolve(request.clone(), stale.clone())
            .await
            .unwrap();
        running.shutdown().await.unwrap();
        assert!(service.resolve(request, stale).await.is_err());
    })
    .await
    .unwrap();
}

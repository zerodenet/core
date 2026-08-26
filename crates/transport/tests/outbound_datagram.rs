use std::sync::Arc;

use zero_platform_tokio::{EgressInterface, EgressInterfaceControl};
use zero_transport::{
    OutboundDatagramSocketFactory, OutboundHostResolveFuture, OutboundHostResolver,
};

#[derive(Debug)]
struct TestResolver;

impl OutboundHostResolver for TestResolver {
    fn resolve(&self, _host: String, port: u16) -> OutboundHostResolveFuture {
        Box::pin(async move {
            Ok(vec![
                std::net::SocketAddr::new("192.0.2.1".parse().unwrap(), port),
                std::net::SocketAddr::new("192.0.2.1".parse().unwrap(), port),
            ])
        })
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn factory_reads_the_current_egress_for_each_new_socket() {
    let control = EgressInterfaceControl::default();
    let factory = OutboundDatagramSocketFactory::new(control.clone());
    let peer = "192.0.2.1:443".parse().unwrap();

    let selected = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();
    control.replace_for(false, Some(selected.clone()));
    assert_eq!(factory.egress_for(peer), Some(selected));

    control.replace_for(false, None);
    assert!(factory.egress_for(peer).is_none());
    let socket = factory
        .bind_std(peer)
        .expect("new socket must observe the reconciled automatic egress");
    assert!(socket.local_addr().unwrap().is_ipv4());
}

#[test]
fn factory_rejects_an_active_tun_without_a_physical_egress() {
    let control = EgressInterfaceControl::default();
    let factory = OutboundDatagramSocketFactory::new(control.clone());
    let peer = "192.0.2.1:443".parse().unwrap();
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);

    let error = factory.bind_std(peer).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
}

#[tokio::test]
async fn factory_resolves_node_domains_through_the_runtime_bridge() {
    let factory = OutboundDatagramSocketFactory::new(EgressInterfaceControl::default())
        .with_host_resolver(Arc::new(TestResolver));

    assert_eq!(
        factory
            .resolve_server_addresses("node.example", 443)
            .await
            .unwrap(),
        vec!["192.0.2.1:443".parse().unwrap()]
    );
}

#[tokio::test]
async fn factory_never_implicitly_uses_the_system_resolver() {
    let factory = OutboundDatagramSocketFactory::new(EgressInterfaceControl::default());

    let error = factory
        .resolve_server_addresses("node.example", 443)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

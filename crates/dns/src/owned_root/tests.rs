//! Negative host admission is not permission to invoke a standalone connector.
use super::*;
use crate::{native::NativeDnsProvider, DnsOutboundConnectFuture, DnsOutboundConnector};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};
use zero_platform_tokio::{
    contract::{scope::TaskScope, BoxFuture, CancellationToken},
    controlled_carrier::loan::{DatagramLoan, StreamLoan},
};

#[derive(Debug, Default)]
struct Standalone(AtomicUsize);
impl DnsOutboundConnector for Standalone {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> DnsOutboundConnectFuture {
        self.0.fetch_add(1, Ordering::AcqRel);
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "standalone connector was called",
            ))
        })
    }
}
// This deliberately grants no native resource. Its real registered query Scope
// lets the test distinguish a denied detour from absence of the original host.
struct NoDetour(OwnedDnsQueryTasks);
impl NativeDnsProvider for NoDetour {
    fn tasks(&self) -> OwnedDnsQueryTasks {
        self.0.clone()
    }
    fn stream(
        &self,
        _: std::net::SocketAddr,
        _: zero_platform_tokio::contract::scope::TaskJoinWitness,
        _: CancellationToken,
        _: tokio::time::Instant,
    ) -> BoxFuture<'_, io::Result<StreamLoan>> {
        Box::pin(async { panic!("unqualified host cannot acquire a native stream") })
    }
    fn datagram(
        &self,
        _: std::net::SocketAddr,
        _: zero_platform_tokio::contract::scope::TaskJoinWitness,
        _: CancellationToken,
        _: tokio::time::Instant,
    ) -> BoxFuture<'_, io::Result<DatagramLoan>> {
        Box::pin(async { panic!("unqualified host cannot acquire a native datagram") })
    }
}
fn config() -> DnsConfig {
    serde_json::from_value(serde_json::json!({
        "servers":{"query":{"type":"udp","host":"127.0.0.1","port":53,"detour":"selected"}},
        "default_server":"query"
    }))
    .unwrap()
}

#[tokio::test]
async fn host_denied_detour_never_becomes_a_standalone_backend_attempt() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let tasks = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let provider: Arc<dyn NativeDnsProvider> = Arc::new(NoDetour(tasks.clone()));
    let source = Arc::new(native::NativeDnsSource::default());
    source.install(&provider).unwrap();
    // Match ordinary Proxy's coordinator binding, including builds which do
    // not set DnsSystem.native_source through build_with_native_queries.
    let dns = DnsSystem::build(Some(&config()))
        .unwrap()
        .with_native_query_queries(source);
    assert!(dns.native_source.is_none());
    let connector = Arc::new(Standalone::default());
    dns.set_outbound_connector(connector.clone());
    assert!(dns.resolve_real_type("actual-query.test", 1).await.is_err());
    assert_eq!(connector.0.load(Ordering::Acquire), 0);
    let originals = tasks.observe();
    assert_eq!(originals.len(), 1);
    assert!(originals[0].original.is_none());
    assert!(scope
        .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await
        .is_complete());
    assert!(originals[0].task.is_joined());
}

#[tokio::test]
async fn an_explicit_standalone_caller_keeps_its_real_connector_responsibility() {
    let dns = DnsSystem::build(Some(&config())).unwrap();
    let connector = Arc::new(Standalone::default());
    dns.set_outbound_connector(connector.clone());
    assert!(dns.resolve_real_type("actual-query.test", 1).await.is_err());
    assert_eq!(connector.0.load(Ordering::Acquire), 1);
    dns.shutdown_queries(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
}

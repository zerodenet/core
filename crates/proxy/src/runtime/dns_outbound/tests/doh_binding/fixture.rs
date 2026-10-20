use super::*;
use crate::runtime::dns_outbound::ProxyDnsOutboundConnector;
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Notify},
    task::JoinHandle,
};
use zero_dns::{
    test_support::{doh_pool_witness, DohPoolWitness},
    DnsConnectContext, DnsOutboundCarrierFuture, DnsOutboundConnectFuture, DnsOutboundConnector,
    DnsSystem,
};
mod peer;
mod socks;
pub use peer::TlsPeer;
pub use socks::SocksPeer;

#[derive(Default)]
pub struct Counts {
    pub accepted: AtomicUsize,
    pub closed: AtomicUsize,
    changed: Notify,
}
impl Counts {
    pub async fn wait_closed(&self, count: usize) {
        tokio::time::timeout_at(until(), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.closed.load(Ordering::Acquire) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("actual peer socket did not close");
    }
    fn record_close(&self) {
        self.closed.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }
}
pub struct Task {
    stop: oneshot::Sender<()>,
    task: JoinHandle<io::Result<()>>,
}
impl Task {
    pub async fn finish(self) {
        let _ = self.stop.send(());
        tokio::time::timeout_at(until(), self.task)
            .await
            .expect("fixture task did not exit")
            .expect("fixture task panicked")
            .expect("fixture I/O failed");
    }
}
/// Preserve terminal peer reset errors as evidence, accepting only EOF/reset/TLS truncation
/// for an explicit hard-close. Every unrelated native failure remains fatal.
fn terminal(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
            ) =>
        {
            tracing::debug!(%source,"actual fixture peer observed native reset");
            Ok(())
        }
        other => other,
    }
}

#[derive(Debug)]
pub struct ObservedConnector {
    actual: ProxyDnsOutboundConnector,
    resolver: Weak<DnsSystem>,
    pub opens: AtomicUsize,
    pub witnesses: Mutex<Vec<DohPoolWitness>>,
}
impl DnsOutboundConnector for ObservedConnector {
    fn connect(
        &self,
        outbound: String,
        endpoint: std::net::SocketAddr,
    ) -> DnsOutboundConnectFuture {
        self.actual.connect(outbound, endpoint)
    }
    fn connect_carrier_with_context(
        &self,
        outbound: String,
        endpoint: std::net::SocketAddr,
        context: DnsConnectContext,
    ) -> DnsOutboundCarrierFuture {
        let resolver = self.resolver.upgrade().expect("actual resolver owner");
        let witness = doh_pool_witness(&resolver, "query").expect("actual configured pool witness");
        assert_eq!(
            witness.registered_driver_tasks, 0,
            "opening a new carrier before old driver join: {witness:?}"
        );
        assert_eq!(
            witness.active_leases, 0,
            "opening a new carrier before old users exit: {witness:?}"
        );
        assert!(
            !witness.closing,
            "new carrier cannot enter a retired backend"
        );
        self.witnesses.lock().unwrap().push(witness);
        self.opens.fetch_add(1, Ordering::AcqRel);
        let work = self
            .actual
            .connect_carrier_with_context(outbound, endpoint, context);
        Box::pin(async move {
            let carrier = work.await?;
            assert!(
                carrier.binding.is_some(),
                "fixture must traverse the real native producer"
            );
            Ok(carrier)
        })
    }
}

pub struct Harness {
    pub proxy: Proxy,
    pub connector: Arc<ObservedConnector>,
    pub socks: [SocksPeer; 2],
    pub dns: TlsPeer,
}
impl Harness {
    pub async fn new(loadbalance: bool) -> Self {
        let dns = TlsPeer::start().await;
        let first = SocksPeer::start(dns.endpoint).await;
        let second = SocksPeer::start(dns.endpoint).await;
        let group = if loadbalance {
            serde_json::json!({"tag":"choose","type":"load_balance","outbounds":["one","two"],"strategy":"round_robin"})
        } else {
            serde_json::json!({"tag":"choose","type":"selector","outbounds":["one","two"],"selected":"one"})
        };
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[
                {"tag":"one","protocol":{"type":"socks5","server":"127.0.0.1","port":first.endpoint.port()}},
                {"tag":"two","protocol":{"type":"socks5","server":"127.0.0.1","port":second.endpoint.port()}}
            ],"outbound_groups":[group],
            "runtime":{"dns":{"servers":{
                "bootstrap":{"type":"system"},
                "query":{"type":"doh","host":"127.0.0.1","port":dns.endpoint.port(),"server_name":"localhost","path":"/dns-query","detour":"choose"}
            },"default_server":"query","policy":{"node_server":"bootstrap","timeout_ms":5000}}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config).unwrap();
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", dns.certificate.clone())
            .unwrap();
        let config = proxy.engine.runtime_snapshot().config().clone();
        proxy
            .protocols
            .start_components(config.clone(), until())
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&config, || Ok(()))
            .unwrap();
        let connector = Arc::new(ObservedConnector {
            actual: ProxyDnsOutboundConnector::new(proxy.current_service_source()),
            resolver: Arc::downgrade(&proxy.resolver),
            opens: AtomicUsize::new(0),
            witnesses: Mutex::new(vec![]),
        });
        proxy.resolver.set_outbound_connector(connector.clone());
        Self {
            proxy,
            connector,
            socks: [first, second],
            dns,
        }
    }
    pub async fn query(&self, name: &str) -> Vec<IpAddress> {
        tokio::time::timeout_at(until(), self.proxy.resolver.resolve_real_type(name, 1))
            .await
            .expect("bounded real DoH query")
            .expect("real DoH response")
    }
    pub fn assert_no_business_sessions(&self) {
        assert!(self.proxy.engine.active_sessions().is_empty());
        assert!(self.proxy.engine.completed_sessions().is_empty());
    }
    pub async fn finish(self) {
        self.proxy.resolver.shutdown_queries(until()).await.unwrap();
        self.proxy
            .resolver
            .shutdown_doh_pools(until())
            .await
            .unwrap();
        self.proxy
            .resolver
            .shutdown_doq_endpoints(until())
            .await
            .unwrap();
        let witness = doh_pool_witness(&self.proxy.resolver, "query").unwrap();
        assert_eq!(witness.registered_driver_tasks, 0);
        assert_eq!(witness.active_leases, 0);
        assert!(witness.closing);
        self.proxy
            .protocols
            .shutdown_components(until())
            .await
            .unwrap();
        let [first, second] = self.socks;
        first.task.finish().await;
        second.task.finish().await;
        self.dns.task.finish().await;
    }
}

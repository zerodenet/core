//! Type 65 runs in the actual Node query Scope and original DNS transport pools.
use super::*;
use crate::{
    coordinator::{QueryKey, QueryScope},
    native::NativeDnsSource,
    DnsQueryRole, OwnedDnsQueryTasks, ResolverServer,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zero_platform_tokio::{contract::dns::DnsGeneration, EgressInterfaceControl};

pub(super) struct Execution {
    endpoint: EchDnsEndpoint,
    source: Arc<NativeDnsSource>,
    #[cfg(feature = "doh")]
    owners: Arc<crate::transport_owners::TransportOwners>,
    #[cfg(feature = "doh")]
    servers: Vec<ResolverServer>,
    egress: EgressInterfaceControl,
    configuration: Arc<AtomicU64>,
    generation: DnsGeneration,
    timeout: std::time::Duration,
}
impl DnsSystem {
    pub(super) fn prepare_owned_ech(&self, server: &str) -> io::Result<Execution> {
        let node = self
            .node_queries
            .as_ref()
            .ok_or_else(|| invalid_endpoint("ECH has no actual Node source"))?;
        let inner = self.inner.read().expect("DNS system lock poisoned");
        let (servers, timeout): (Vec<ResolverServer>, u64) = match &*inner {
            crate::DnsSystemInner::Configured {
                servers, policy, ..
            } => (servers.values().cloned().collect(), policy.timeout_ms),
            _ => (
                Vec::new(),
                zero_config::DnsPolicyConfig::default().timeout_ms,
            ),
        };
        #[cfg(not(feature = "doh"))]
        let _ = servers;
        let generation = DnsGeneration::new(
            self.config_generation.load(Ordering::Acquire),
            self.egress_interface.generation(),
        );
        Ok(Execution {
            endpoint: EchDnsEndpoint::parse(server)?,
            source: node.source.clone(),
            #[cfg(feature = "doh")]
            owners: self.transport_owners.clone(),
            #[cfg(feature = "doh")]
            servers,
            egress: self.egress_interface.clone(),
            configuration: self.config_generation.clone(),
            generation,
            timeout: std::time::Duration::from_millis(timeout),
        })
    }
    pub(super) async fn query_owned_ech(
        &self,
        name: &str,
        server: &str,
    ) -> io::Result<EchDnsRecord> {
        let name = crate::message::normalize_domain(name)?;
        let node = self
            .node_queries
            .as_ref()
            .ok_or_else(|| invalid_endpoint("ECH has no actual Node source"))?;
        node.wire.ensure_local_open()?;
        if crate::localhost::is_localhost(&name) {
            return Ok(EchDnsRecord {
                config_list: None,
                ttl_seconds: crate::message::DEFAULT_NEGATIVE_TTL_SECONDS,
            });
        }
        let execution = self.prepare_owned_ech(server)?;
        let mut scope = QueryScope::new(DnsQueryRole::Node, execution.generation.network.0);
        scope.config_generation = execution.generation.resolver;
        let query = crate::message::build_query(&name, crate::message::TYPE_HTTPS)?;
        let id = [query[0], query[1]];
        let key = QueryKey::new(&name, crate::message::TYPE_HTTPS, scope)
            .with_wire_query(&query)
            .with_endpoint(execution.endpoint.as_str());
        let original = query.clone();
        let tasks = node.source.tasks()?;
        let mut response = tasks
            .bootstrap(
                node.wire
                    .resolve(key, async move { execution.exchange(&query).await }),
            )
            .await??;
        response[..2].copy_from_slice(&id);
        parsed(&original, &response)
    }
}
impl Execution {
    pub(super) fn generation(&self) -> DnsGeneration {
        self.generation
    }
    pub(super) fn tasks(&self) -> io::Result<OwnedDnsQueryTasks> {
        self.source.tasks()
    }
    fn check(&self) -> io::Result<()> {
        if self.configuration.load(Ordering::Acquire) != self.generation.resolver
            || self.egress.generation() != self.generation.network.0
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "ECH belongs to a retired DNS/network generation",
            ));
        }
        Ok(())
    }
    pub(super) async fn query(self, name: &str) -> io::Result<EchDnsRecord> {
        let name = crate::message::normalize_domain(name)?;
        self.check()?;
        if crate::localhost::is_localhost(&name) {
            return Ok(EchDnsRecord {
                config_list: None,
                ttl_seconds: crate::message::DEFAULT_NEGATIVE_TTL_SECONDS,
            });
        }
        let query = crate::message::build_query(&name, crate::message::TYPE_HTTPS)?;
        let response = self.exchange(&query).await?;
        parsed(&query, &response)
    }
    async fn exchange(self, query: &[u8]) -> io::Result<Vec<u8>> {
        self.check()?;
        OwnedDnsQueryTasks::bind_ech_endpoint(&self.endpoint)?;
        let caller = OwnedDnsQueryTasks::current_witness()?;
        let cancel = OwnedDnsQueryTasks::current_cancellation()?;
        let end = tokio::time::Instant::now() + self.timeout;
        let mut network = self.egress.subscribe_generation();
        let work = async {
            let peers = self
                .source
                .provider()?
                .prepare_ech_endpoint(self.endpoint.clone(), caller, cancel.clone(), end)
                .await?;
            if peers.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "ECH endpoint has no actual Node addresses",
                ));
            }
            if self.endpoint.is_datagram() {
                #[cfg(feature = "udp")]
                {
                    let mut last = None;
                    for peer in peers {
                        let attempt = self.source.exchange_ech_udp(peer, query, end).await;
                        let (result, retry) = attempt.into_parts();
                        if result.is_ok() || !retry {
                            return result;
                        }
                        last = Some(result);
                    }
                    return last.expect("nonempty actual ECH addresses");
                }
                #[cfg(not(feature = "udp"))]
                return Err(unsupported("UDP"));
            }
            #[cfg(feature = "doh")]
            {
                let backend = super::pool::backend(
                    &self.endpoint,
                    &peers,
                    &self.servers,
                    &self.owners,
                    &self.egress,
                )?;
                let crate::backends::ResolverBackend::Doh(resolver) = backend.as_ref() else {
                    return Err(invalid_endpoint("ECH backend is not DoH"));
                };
                resolver
                    .exchange_native(self.source.provider()?, None, query, end)
                    .await
                    .into_parts()
                    .0
            }
            #[cfg(not(feature = "doh"))]
            Err(unsupported("DNS-over-HTTPS"))
        };
        let result = tokio::select! { biased;
            _ = cancel.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted, "actual ECH query cancelled")),
            _ = network.changed() => Err(io::Error::new(io::ErrorKind::Interrupted, "ECH network generation changed")),
            result = tokio::time::timeout_at(end, work) => result.map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?,
        };
        self.check()?;
        result
    }
}
fn parsed(query: &[u8], response: &[u8]) -> io::Result<EchDnsRecord> {
    let answer = crate::message::parse_ech_config_response(query, response)?;
    Ok(EchDnsRecord {
        config_list: answer.config_list,
        ttl_seconds: answer.ttl_seconds,
    })
}

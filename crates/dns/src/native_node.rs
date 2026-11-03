//! Owned Node requests share the actual resolver policy, never legacy flights.
use super::*;
use std::sync::Weak;
use zero_platform_tokio::contract::{
    context::with_deadline,
    dns::{DnsGeneration, DnsQueryContext, DnsRequest, DnsResolution, DnsService},
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};
pub(super) struct NodeQueries {
    pub(super) source: Arc<native::NativeDnsSource>,
    pub(super) coordinator: QueryCoordinator<Vec<IpAddress>>,
}
impl DnsSystem {
    /// Installs metadata only. The actual RuntimeHost must start its original
    /// DNS provider before this service can acquire any query socket or task.
    pub fn with_native_node_queries(mut self, source: Arc<native::NativeDnsSource>) -> Self {
        let mut coordinator =
            QueryCoordinator::with_graph_and_tasks(self.query_coordinator.shared_graph(), None);
        coordinator.set_native_source(source.clone());
        self.node_queries = Some(NodeQueries {
            source,
            coordinator,
        });
        self
    }
    pub fn native_node_service(self: &Arc<Self>) -> Arc<dyn DnsService> {
        Arc::new(NodeService(Arc::downgrade(self)))
    }
}
struct NodeService(Weak<DnsSystem>);
fn failure(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Dial, reason, Impact::Session);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
impl DnsService for NodeService {
    fn generation(&self) -> DnsGeneration {
        self.0
            .upgrade()
            .map(|dns| dns.generation())
            .unwrap_or(DnsGeneration::new(u64::MAX, u64::MAX))
    }
    fn resolve(
        &self,
        request: DnsRequest,
        context: DnsQueryContext,
    ) -> BoxFuture<'_, Result<DnsResolution, ChannelError>> {
        Box::pin(async move {
            let dns = self.0.upgrade().ok_or_else(|| failure(Reason::Revoked))?;
            if request.role != DnsQueryRole::Node {
                return Err(failure(Reason::Unsupported));
            }
            let generation = dns.generation();
            if context
                .expected_generation
                .is_some_and(|expected| expected != generation)
            {
                return Err(failure(Reason::StalePlan));
            }
            let node = dns
                .node_queries
                .as_ref()
                .ok_or_else(|| failure(Reason::Unsupported))?;
            let mut snapshot = dns.snapshot().ok_or_else(|| failure(Reason::Unsupported))?;
            // No detour or System backend may escape this explicit native entry.
            for server in snapshot.servers.values() {
                native::validate_node_server(&server.config)
                    .map_err(|source| failure(Reason::Unsupported).with_source(source))?;
            }
            snapshot
                .ensure_query_role(DnsQueryRole::Node)
                .map_err(|source| failure(Reason::Unsupported).with_source(source))?;
            snapshot.native_source = Some(node.source.clone());
            snapshot.query_coordinator = node.coordinator.clone();
            // Native answers never borrow legacy Node proof, but both namespaces
            // share the actual configured cache's one total capacity/TTL budget.
            snapshot.cache = snapshot.cache.as_ref().map(DnsCache::isolated);
            snapshot.family = request.family;
            match request.family {
                AddressFamily::OnlyIpv4 => {
                    snapshot.policy.address_family = DnsAddressFamilyPolicy::Ipv4Only
                }
                AddressFamily::OnlyIpv6 => {
                    snapshot.policy.address_family = DnsAddressFamilyPolicy::Ipv6Only
                }
                AddressFamily::Auto => {}
            }
            let domain = message::normalize_domain(&request.name)
                .map_err(|source| failure(Reason::InvalidConfig).with_source(source))?;
            let mut network = dns.egress_interface.subscribe_generation();
            if *network.borrow() != generation.network.0 {
                return Err(failure(Reason::StalePlan));
            }
            let work = resolve_snapshot(&domain, DnsQueryRole::Node, snapshot);
            let addresses = tokio::select! {biased;
                _=context.cancel.cancelled()=>return Err(failure(Reason::Cancelled)),
                _=network.changed()=>return Err(failure(Reason::StalePlan)),
                value=with_deadline(context.deadline,work)=>value.map_err(|source|failure(Reason::Timeout).with_source(source))?
                    .map_err(|source|failure(Reason::Io).with_source(source))?,
            };
            if dns.generation() != generation || context.cancel.is_cancelled() {
                return Err(failure(Reason::StalePlan));
            }
            Ok(DnsResolution {
                addresses,
                generation,
            })
        })
    }
}

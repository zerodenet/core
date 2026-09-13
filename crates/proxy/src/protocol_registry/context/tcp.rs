mod execution;
use super::{OutboundAdapterContext, UpstreamConnectServices};
use crate::inventory::ProtocolInventory;
use crate::runtime::principal_rate_limit::PrincipalRateLimitRegistry;
pub(crate) use execution::TcpExecutionServices;
use std::sync::Arc;
use zero_dns::DnsSystem;
use zero_engine::{Engine, EngineRuntimeSnapshot};

#[derive(Clone)]
pub(crate) struct TcpRuntimeServices {
    execution: TcpExecutionServices,
    protocols: ProtocolInventory,
}
impl std::ops::Deref for TcpRuntimeServices {
    type Target = TcpExecutionServices;
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
}
impl TcpRuntimeServices {
    pub(crate) fn execution(&self) -> TcpExecutionServices {
        self.execution.clone()
    }
    pub(crate) fn new(
        engine: Engine,
        snapshot: Arc<EngineRuntimeSnapshot>,
        resolver: Arc<DnsSystem>,
        protocols: ProtocolInventory,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
        principal_rate_limits: PrincipalRateLimitRegistry,
    ) -> Self {
        Self {
            execution: TcpExecutionServices {
                engine,
                snapshot,
                upstream: UpstreamConnectServices::new(
                    resolver,
                    protocols.direct_connector(),
                    egress_interface,
                ),
                principal_rate_limits,
            },
            protocols,
        }
    }

    pub(crate) fn with_current_snapshot(&self) -> Self {
        let mut services = self.clone();
        services.execution.snapshot = services.engine.runtime_snapshot();
        services
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn protocols(&self) -> &ProtocolInventory {
        &self.protocols
    }

    pub(crate) fn prepare_tcp_outbound<'a>(
        &'a self,
        resolved: &'a zero_engine::ResolvedOutbound<'a>,
    ) -> Result<crate::inventory::PreparedTcpOutbound, crate::transport::TcpOutboundFailure> {
        self.protocols
            .prepare_tcp_outbound(OutboundAdapterContext::new(self.config()), resolved)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain,
    ) -> Result<crate::transport::RelayCarrier, crate::transport::TcpOutboundFailure> {
        crate::runtime::tcp_dispatch::relay::dispatch_prepared_tcp_relay_carrier(
            self.execution(),
            prepared,
        )
        .await
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_lazy_tcp_relay_carrier<'a>(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain,
    ) -> crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a> {
        crate::runtime::tcp_dispatch::relay::prepare_lazy_tcp_relay_carrier(
            self.execution(),
            prepared,
        )
    }
}

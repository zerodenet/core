use zero_engine::{EngineError, RouteMode};

use super::super::ProtocolInventory;
use crate::inventory::ClaimedInventoryLeaf;
use crate::protocol_registry::OutboundAdapterContext;
use crate::runtime::path::TcpPathCategory;
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
use crate::transport::TcpOutboundFailure;

#[derive(Clone)]
pub(crate) struct PreparedTcpCandidate {
    pub(crate) health_tag: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) protocol: String,
    pub(crate) endpoint: Option<(String, u16)>,
    pub(crate) execution: PreparedTcpCandidateExecution,
}

#[derive(Clone)]
pub(crate) enum PreparedTcpCandidateExecution {
    Native(std::sync::Arc<crate::component::instance::ComponentSlot>),
    Block { tag: String },
    Connect(std::sync::Arc<dyn PreparedTcpConnectOperation>),
}

#[derive(Clone)]
pub(crate) struct PreparedTcpRelayHop {
    pub(crate) tag: String,
    pub(crate) protocol: String,
    pub(crate) server: String,
    pub(crate) port: u16,
    pub(crate) execution: PreparedTcpRelayExecution,
}

#[derive(Clone)]
pub(crate) enum PreparedTcpRelayExecution {
    Native(super::native::StreamRoleRef),
    Legacy(std::sync::Arc<dyn PreparedTcpRelayOperation>),
}

impl PreparedTcpRelayHop {
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn upstream(&self) -> (String, u16) {
        (self.server.clone(), self.port)
    }
}

impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_claimed_tcp_candidate<'a>(
        &self,
        _ctx: OutboundAdapterContext,
        claimed: &ClaimedInventoryLeaf<'a>,
        mode: RouteMode,
    ) -> Result<PreparedTcpCandidate, TcpOutboundFailure> {
        let runtime = claimed.runtime();
        let health_tag = health_tag(&runtime).map(ToOwned::to_owned);
        let execution = if matches!(runtime.tcp_path, TcpPathCategory::Block) {
            PreparedTcpCandidateExecution::Block {
                tag: runtime.kernel_tag.unwrap_or_else(|| "block".to_owned()),
            }
        } else {
            let operation = claimed.tcp_connect_for_route(mode)?;
            PreparedTcpCandidateExecution::Connect(operation)
        };
        Ok(PreparedTcpCandidate {
            health_tag,
            tag: runtime.tag,
            protocol: runtime.protocol,
            endpoint: runtime
                .endpoint
                .map(|endpoint| (endpoint.server, endpoint.port)),
            execution,
        })
    }

    pub(in crate::inventory) fn prepare_claimed_tcp_relay_hop<'a>(
        &self,
        _ctx: OutboundAdapterContext,
        claimed: &ClaimedInventoryLeaf<'a>,
    ) -> Result<PreparedTcpRelayHop, EngineError> {
        let (server, port, operation) = claimed.tcp_relay_hop()?;
        let runtime = claimed.runtime();
        Ok(PreparedTcpRelayHop {
            tag: runtime.tag.unwrap_or_else(|| "unknown".to_owned()),
            protocol: runtime.protocol,
            server,
            port,
            execution: PreparedTcpRelayExecution::Legacy(operation),
        })
    }
}

fn health_tag(runtime: &crate::protocol_registry::OutboundLeafRuntime) -> Option<&str> {
    match runtime.tcp_path {
        TcpPathCategory::Unavailable | TcpPathCategory::Direct | TcpPathCategory::Block => None,
        #[cfg(feature = "tcp-tunnel-runtime")]
        TcpPathCategory::Tunnel => runtime.health_tag.as_deref(),
        #[cfg(feature = "tcp-session-runtime")]
        TcpPathCategory::Session => runtime.health_tag.as_deref(),
        #[cfg(feature = "tcp-transport-session-runtime")]
        TcpPathCategory::TransportSession => runtime.health_tag.as_deref(),
    }
}

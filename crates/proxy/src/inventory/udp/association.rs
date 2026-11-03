//! Project configured endpoint facts onto one actual compiled Datagram generation.
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamAssociationTarget;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

/// Inventory selects the migrated production path; runtime sees only the
/// actual generation and endpoint, never a protocol claim or config variant.
#[cfg(feature = "upstream-association-runtime")]
pub(super) fn prepare(
    ctx: &crate::protocol_registry::UdpAdapterContext<'_>,
    leaf: &zero_engine::ResolvedLeafOutbound<'_>,
) -> Option<Result<DatagramAssociationTarget, crate::runtime::udp_dispatch::FlowFailure>> {
    let zero_engine::ResolvedLeafOutbound::Proxy { identity } = leaf else {
        return None;
    };
    let input = ctx.config().outbounds.get(identity.config_index())?;
    // Pooled roles retain their existing gate until their business path qualifies.
    if !matches!(
        input.protocol,
        zero_config::OutboundProtocolConfig::Socks5 { .. }
    ) {
        return None;
    }
    let (server, port) = input.protocol.endpoint()?;
    let failure = |stage, error| crate::runtime::udp_dispatch::FlowFailure {
        stage,
        error,
        upstream: Some((server.into(), port)),
    };
    if !ctx.udp_enabled_for_outbound(Some(&input.tag)) {
        return Some(Err(failure(
            "udp_policy",
            std::io::Error::other("udp disabled for outbound").into(),
        )));
    }
    Some(
        DatagramAssociationTarget::resolve(
            &ctx.packet_path_execution_services().tcp(),
            &input.tag,
            server,
            port,
        )
        .map_err(|error| failure("udp_upstream_send", error)),
    )
}

/// Per-flow admission follows actual compiled factory roles. Association-style
/// roles are selected first; unavailable roles retain their compatibility path.
#[cfg(feature = "managed-stream-runtime")]
pub(super) fn prepare_flow(
    ctx: &crate::protocol_registry::UdpAdapterContext<'_>,
    leaf: &zero_engine::ResolvedLeafOutbound<'_>,
) -> Option<Result<DatagramAssociationTarget, crate::runtime::udp_dispatch::FlowFailure>> {
    let zero_engine::ResolvedLeafOutbound::Proxy { identity } = leaf else {
        return None;
    };
    let input = ctx.config().outbounds.get(identity.config_index())?;
    let execution = ctx.packet_path_execution_services().tcp();
    let plan = execution.components.as_ref().ok()?;
    let slot = plan.configured(&input.tag)?;
    if !slot
        .recipe
        .spec()
        .roles
        .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram)
    {
        return None;
    }
    let (server, port) = input.protocol.endpoint()?;
    let failure = |stage, error| crate::runtime::udp_dispatch::FlowFailure {
        stage,
        error,
        upstream: Some((server.into(), port)),
    };
    if !ctx.udp_enabled_for_outbound(Some(&input.tag)) {
        return Some(Err(failure(
            "udp_policy",
            std::io::Error::other("udp disabled for outbound").into(),
        )));
    }
    Some(
        DatagramAssociationTarget::resolve(
            &ctx.packet_path_execution_services().tcp(),
            &input.tag,
            server,
            port,
        )
        .map_err(|error| failure("udp_datagram_open", error)),
    )
}

/// Host-resolved, secret-free identity for an existing standard Datagram role.
/// Never retain protocol configuration or a strong role/pool in a flow handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatagramAssociationTarget {
    tag: String,
    server: String,
    port: u16,
    instance: zero_core::component::ComponentInstanceId,
}
impl DatagramAssociationTarget {
    pub(crate) fn endpoint(&self) -> (&str, &str, u16) {
        (&self.tag, &self.server, self.port)
    }
    pub(crate) fn resolve(
        execution: &crate::protocol_registry::TcpExecutionServices,
        tag: &str,
        server: &str,
        port: u16,
    ) -> Result<Self, zero_engine::EngineError> {
        let plan = execution.components.as_ref().map_err(|error| {
            zero_engine::EngineError::Io(std::io::Error::other(
                crate::component::instance::retained(error.clone()),
            ))
        })?;
        let slot = plan
            .configured(tag)
            .ok_or_else(|| invalid(Reason::Unsupported, "upstream Datagram role is unavailable"))?;
        let target = Self {
            tag: tag.into(),
            server: server.into(),
            port,
            instance: slot.instance_id(),
        };
        target.current_slot(execution)?;
        Ok(target)
    }
    pub(crate) fn current_slot(
        &self,
        execution: &crate::protocol_registry::TcpExecutionServices,
    ) -> Result<std::sync::Arc<crate::component::instance::ComponentSlot>, zero_engine::EngineError>
    {
        if let Some((reason, endpoint)) =
            zero_engine::EndpointAdmission::from_config(execution.config())
                .outbound_denial(&self.tag)
        {
            return Err(zero_engine::EngineError::InvalidPlan {
                message: format!("{reason}: {endpoint}"),
            });
        }
        let input = execution
            .config()
            .outbounds
            .iter()
            .find(|input| input.tag == self.tag)
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "upstream Datagram configuration was retired",
                )
            })?;
        if input.protocol.endpoint() != Some((&self.server, self.port)) {
            return Err(invalid(
                Reason::StalePlan,
                "upstream Datagram endpoint was retired",
            ));
        }
        let plan = execution.components.as_ref().map_err(|error| {
            zero_engine::EngineError::Io(std::io::Error::other(
                crate::component::instance::retained(error.clone()),
            ))
        })?;
        let slot = plan
            .configured(&self.tag)
            .ok_or_else(|| invalid(Reason::Unsupported, "upstream Datagram role is unavailable"))?;
        if slot.instance_id() != self.instance {
            return Err(invalid(
                Reason::StalePlan,
                "upstream Datagram generation was retired",
            ));
        }
        Ok(slot)
    }
}
#[cfg(feature = "upstream-association-runtime")]
impl UpstreamAssociationTarget for DatagramAssociationTarget {
    fn outbound_tag(&self) -> &str {
        &self.tag
    }
    fn log_parts(&self) -> (&str, &str, u16) {
        (&self.tag, &self.server, self.port)
    }
}
fn invalid(reason: Reason, message: &'static str) -> zero_engine::EngineError {
    let error = ChannelError::new(Stage::Prepare, reason, Impact::Session)
        .with_source(std::io::Error::other(message));
    zero_engine::EngineError::Io(std::io::Error::other(error))
}

#[cfg(test)]
#[path = "association/tests.rs"]
mod tests;

#[cfg(all(test, feature = "managed-stream-runtime"))]
#[path = "association/tests/vmess.rs"]
mod vmess_tests;

//! Standard cold Packet endpoint declaration over the original UDP grant.
use super::inbound::cold::{Factory, Options};
use crate::protocol_registry::PreparedInboundBind;
use std::{net::IpAddr, num::NonZeroUsize, sync::Arc};
use zero_config::InboundConfig;
use zero_core::{request::Destination, Address};
use zero_engine::EngineError;
use zero_platform_tokio::contract::component::*;
pub(super) fn destination(inbound: &InboundConfig) -> Result<Destination, EngineError> {
    let endpoint = zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port)
        .ok_or_else(|| super::invalid("WireGuard standard endpoint requires a literal address"))?;
    let ip = endpoint.ip();
    Ok(Destination::new(
        match ip {
            IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        inbound.listen.port,
    ))
}
pub(super) fn spec_with_limits(
    inbound: &InboundConfig,
    limits: crate::runtime::inbound_operation::raw_ip::RawIpSourceLimits,
) -> Result<ComponentSpec, EngineError> {
    let destination = destination(inbound).expect("validated literal endpoint");
    let mut identity = super::inbound_protocol_identity(&inbound.protocol)
        .expect("serializable protocol")
        .to_vec();
    for value in limits.wire_values() {
        identity.extend_from_slice(&(value as u64).to_le_bytes());
    }
    Ok(ComponentSpec {
        owner: ResourceOwner::Component,
        roles: vec![RoleKind::Inbound],
        dependencies: vec![],
        static_identity: identity.into(),
        task_limit: NonZeroUsize::new(limits.task_limit(false).map_err(std::io::Error::other)?),
        resources: vec![
            ResourceDeclaration::DatagramListener {
                accepted: None,
                destination,
                max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                max_pending_sends: NonZeroUsize::new(1).unwrap(),
            },
            ResourceDeclaration::NativeIcmp {
                max_sockets: NonZeroUsize::new(limits.icmp).unwrap(),
            },
            ResourceDeclaration::PacketDatagramReads {
                datagram: 0,
                workspaces: [NonZeroUsize::new(65_536).unwrap(); 2],
                control_workspace: NonZeroUsize::new(148),
                supplemental: Default::default(),
            },
        ],
    })
}
pub(super) fn cold_bind(options: Options) -> Result<PreparedInboundBind, EngineError> {
    destination(&options.inbound)?;
    let recovery = options.recovery.clone();
    PreparedInboundBind::from_recipe(crate::component::ComponentRecipe::from_validated(
        Arc::new(Factory),
        Arc::new(options),
    ))
    .map(|bind| bind.with_recovery(recovery))
}

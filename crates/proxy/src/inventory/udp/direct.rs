//! Engine-selected Direct facts. No protocol executor, runtime or native grant.
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::FlowFailure;
use zero_engine::RouteMode;

pub(crate) struct DirectDatagramTarget {
    pub(crate) tag: String,
    pub(crate) policy_tag: Option<String>,
    pub(crate) dial_policy: zero_traits::DialPolicy,
    pub(crate) dial_generation: u64,
}
impl DirectDatagramTarget {
    pub(super) fn prepare(
        ctx: &UdpAdapterContext<'_>,
        tag: Option<&str>,
        dial_policy: &zero_traits::DialPolicy,
        dial_generation: u64,
        mode: RouteMode,
    ) -> Result<Self, FlowFailure> {
        if !ctx.udp_enabled_for_outbound(tag) {
            return Err(FlowFailure {
                stage: "udp_policy",
                error: std::io::Error::other("udp disabled for outbound").into(),
                upstream: None,
            });
        }
        if mode == RouteMode::Packet {
            return Err(crate::runtime::path::missing_udp_route_capability(mode));
        }
        zero_platform_tokio::validate_dial_policy(dial_policy).map_err(|error| FlowFailure {
            stage: "validate_dial_policy",
            error: error.into(),
            upstream: None,
        })?;
        Ok(Self {
            tag: tag.unwrap_or("direct").into(),
            policy_tag: tag.map(str::to_owned),
            dial_policy: dial_policy.clone(),
            dial_generation,
        })
    }
}

#[cfg(test)]
#[path = "direct/tests.rs"]
mod tests;

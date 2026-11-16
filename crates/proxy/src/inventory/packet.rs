//! Automatic data-plane choice after the engine has selected an outbound.

use zero_config::RuntimeConfig;
use zero_engine::RouteMode;
use zero_engine::{ResolvedLeafOutbound, ResolvedOutbound};
use zero_stack::packet::{IPPROTO_ICMP, IPPROTO_ICMPV6, IPPROTO_TCP, IPPROTO_UDP};

use super::ProtocolInventory;

pub(crate) enum PacketRouteTarget {
    Packet {
        tag: String,
        translated: bool,
        role: zero_core::component::RoleInstanceId,
    },
    Flow,
    DirectEcho {
        dial_policy: zero_traits::DialPolicy,
    },
    Block,
    Unsupported,
    Fallback(Vec<PacketRouteTarget>),
}

impl PacketRouteTarget {
    pub(crate) fn into_candidates(self) -> Vec<Self> {
        match self {
            Self::Fallback(candidates) => candidates,
            single => vec![single],
        }
    }
}

impl ProtocolInventory {
    #[cfg(test)]
    pub(crate) fn prepare_packet_route_target_with_mode(
        &self,
        config: &RuntimeConfig,
        resolved: ResolvedOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
    ) -> PacketRouteTarget {
        self.prepare_packet_route_target_admitted(
            config,
            resolved,
            protocol,
            mode,
            &zero_engine::EndpointAdmission::from_config(config),
            None,
        )
    }

    pub(crate) fn prepare_packet_route_target_admitted(
        &self,
        config: &RuntimeConfig,
        resolved: ResolvedOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
        admission: &zero_engine::EndpointAdmission<'_>,
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> PacketRouteTarget {
        let leaf = match resolved {
            ResolvedOutbound::Single(leaf) => leaf,
            ResolvedOutbound::Fallback { candidates } => {
                return PacketRouteTarget::Fallback(
                    candidates
                        .into_iter()
                        .map(|leaf| {
                            self.prepare_packet_leaf(
                                config, leaf, protocol, mode, admission, components,
                            )
                        })
                        .collect(),
                );
            }
            ResolvedOutbound::Relay { .. } => {
                return if mode != RouteMode::Packet
                    && matches!(protocol, Some(IPPROTO_TCP | IPPROTO_UDP))
                {
                    PacketRouteTarget::Flow
                } else {
                    PacketRouteTarget::Unsupported
                };
            }
        };
        self.prepare_packet_leaf(config, leaf, protocol, mode, admission, components)
    }

    fn prepare_packet_leaf(
        &self,
        config: &RuntimeConfig,
        leaf: ResolvedLeafOutbound<'_>,
        protocol: Option<u8>,
        mode: RouteMode,
        admission: &zero_engine::EndpointAdmission<'_>,
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> PacketRouteTarget {
        use zero_platform_tokio::contract::component::RoleKind;
        let flow_protocol = matches!(protocol, Some(IPPROTO_TCP | IPPROTO_UDP));
        if matches!(leaf, ResolvedLeafOutbound::Block { .. }) {
            return if mode != RouteMode::Packet && flow_protocol {
                PacketRouteTarget::Flow
            } else {
                PacketRouteTarget::Block
            };
        }
        let (tag, slot, direct, policy) = match &leaf {
            ResolvedLeafOutbound::Direct {
                tag, dial_policy, ..
            } => (
                tag.map(str::to_owned).unwrap_or_else(|| "direct".into()),
                components
                    .and_then(|plan| plan.direct(*tag).ok())
                    .filter(|slot| slot.dial == *dial_policy),
                true,
                dial_policy.clone(),
            ),
            ResolvedLeafOutbound::Proxy { identity } => {
                let Some(input) = config.outbounds.get(identity.config_index()) else {
                    return PacketRouteTarget::Unsupported;
                };
                if admission.outbound_denial(input.tag()).is_some() {
                    return PacketRouteTarget::Unsupported;
                }
                (
                    input.tag().to_owned(),
                    components.and_then(|plan| plan.configured(input.tag())),
                    false,
                    input.dial.to_policy(),
                )
            }
            ResolvedLeafOutbound::Block { .. } => unreachable!(),
        };
        // Selection retains the exact installed role, never a protocol plan,
        // private pool or callback to be patched with a provider afterwards.
        let native = slot.as_ref().filter(|slot| {
            (!direct || policy == zero_traits::DialPolicy::default())
                && components.is_some_and(|plan| plan.has_packet_provider(slot.role_id()))
        });
        let translated = native.is_some_and(|slot| {
            components.is_some_and(|plan| plan.has_device_carrier_provider(slot.role_id()))
        });
        if mode == RouteMode::Translate && matches!(protocol, Some(IPPROTO_ICMP | IPPROTO_ICMPV6)) {
            return match native.filter(|_| translated) {
                Some(slot) => PacketRouteTarget::Packet {
                    tag,
                    translated: true,
                    role: slot.role_id(),
                },
                None => PacketRouteTarget::Unsupported,
            };
        }
        // Existing native providers expose endpoint returns. Outbound-only
        // endpoints still need their correlated L4 conversion; translation is
        // the owning stack's separate opt-in, never a forged native guarantee.
        let unsafe_returns = admission.requires_correlated_packet_returns(&tag);
        let forced_flow = matches!(mode, RouteMode::Flow | RouteMode::Translate)
            || (mode == RouteMode::Auto && unsafe_returns);
        if !forced_flow {
            if let Some(slot) = native.filter(|_| !unsafe_returns) {
                return PacketRouteTarget::Packet {
                    tag,
                    translated: false,
                    role: slot.role_id(),
                };
            }
        }
        if mode == RouteMode::Packet {
            return PacketRouteTarget::Unsupported;
        }
        if flow_protocol {
            // Engine-owned Direct semantics remain available. Dispatch uses
            // its actual Stream role or Host Bound Datagram provider.
            if direct {
                return PacketRouteTarget::Flow;
            }
            if slot.as_ref().is_some_and(|slot| match protocol {
                Some(IPPROTO_TCP) => slot.recipe.spec().roles.contains(&RoleKind::Stream),
                Some(IPPROTO_UDP) => slot.recipe.spec().roles.contains(&RoleKind::Datagram),
                _ => false,
            }) {
                return PacketRouteTarget::Flow;
            }
            // Preserve the explicitly deferred process/plugin L4 capability.
            // Such a claim cannot advertise or execute a native Packet path.
            if let Ok(claimed) = self.claim_outbound_leaf(config, leaf) {
                let supported = match protocol {
                    Some(IPPROTO_TCP) => claimed.tcp_connect_for_route(mode).is_ok(),
                    Some(IPPROTO_UDP) => claimed.into_udp_flow_for_route(mode).is_ok(),
                    _ => false,
                };
                if supported {
                    return PacketRouteTarget::Flow;
                }
            }
        }
        if direct
            && mode == RouteMode::Auto
            && policy == zero_traits::DialPolicy::default()
            && config.runtime.network.direct_packet_device.is_none()
            && matches!(protocol, Some(IPPROTO_ICMP | IPPROTO_ICMPV6))
        {
            return PacketRouteTarget::DirectEcho {
                dial_policy: policy,
            };
        }
        PacketRouteTarget::Unsupported
    }
}

#[cfg(test)]
#[path = "packet/tests.rs"]
mod tests;

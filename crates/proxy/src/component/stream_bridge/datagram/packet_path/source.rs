//! One concrete standard-role graph for original Source and Business callers.
//! No claim/prepared-operation executor, private pool or fallback is retained.
use super::*;
use crate::{
    inventory::{service::ServiceNode, PreparedNativeTcpRelayChain},
    runtime::udp_flow::packet_path::UdpDatagramSource,
};

enum Root {
    Datagram(Arc<StandardPacketPath>),
    Associated {
        role: Arc<StandardPacketPath>,
        prefix: PreparedNativeTcpRelayChain,
    },
}
pub(crate) struct RolePacketPath {
    root: Root,
    binding: UpstreamBinding,
    formatters: Vec<UdpDatagramSource>,
}
impl RolePacketPath {
    pub(crate) fn prepare(
        slot: &Arc<ComponentSlot>,
        projection: &crate::protocol_registry::ColdPacketPathProjection,
        prefix: Option<PreparedNativeTcpRelayChain>,
        plan: &crate::component::catalog::ComponentPlan,
        network_generation: NetworkGeneration,
    ) -> Result<Self, EngineError> {
        if !slot
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram)
        {
            return Err(failure(invalid(
                Reason::Unsupported,
                "Source root has no actual Datagram role",
            )));
        }
        let role = StandardPacketPath::prepare_projection(slot, projection);
        let (root, binding) = match prefix {
            Some(prefix) => {
                if !role.associated {
                    return Err(failure(invalid(
                        Reason::Unsupported,
                        "Source root has no Associated grammar",
                    )));
                }
                let nodes = Self::nodes(plan, &prefix)?;
                let binding = UpstreamBinding {
                    prefix: nodes.iter().map(|node| node.slot.role_id()).collect(),
                    dial: slot.dial.clone(),
                    network_generation,
                };
                (Root::Associated { role, prefix }, binding)
            }
            None => {
                let binding = UpstreamBinding {
                    prefix: vec![],
                    dial: slot.dial.clone(),
                    network_generation,
                };
                (Root::Datagram(role), binding)
            }
        };
        Ok(Self {
            root,
            binding,
            formatters: Vec::new(),
        })
    }
    pub(crate) fn descriptor(&self) -> PacketPathCarrierDescriptor {
        let role = match &self.root {
            Root::Datagram(role) | Root::Associated { role, .. } => role,
        };
        let mut descriptor = role.descriptor.clone();
        // Cached business paths must distinguish the actual root, donors,
        // network epoch and intermediate codec families, even at one endpoint.
        use sha2::{Digest, Sha256};
        let mut identity = Sha256::new();
        identity.update(format!("{:?}:{:?}", role.selected.instance(), self.binding));
        identity.update((descriptor.cache_key.len() as u64).to_le_bytes());
        identity.update(descriptor.cache_key.as_bytes());
        for source in &self.formatters {
            let key = &source.descriptor().cache_key;
            identity.update((key.len() as u64).to_le_bytes());
            identity.update(key.as_bytes());
        }
        descriptor.cache_key = format!("standard-role-packet:{:x}", identity.finalize());
        descriptor
    }
    pub(crate) fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
    pub(crate) fn append_formatter(&mut self, source: UdpDatagramSource) {
        self.formatters.push(source);
    }

    fn nodes(
        plan: &crate::component::catalog::ComponentPlan,
        prefix: &PreparedNativeTcpRelayChain,
    ) -> Result<Vec<ServiceNode>, EngineError> {
        if prefix.nodes.is_empty() {
            return Err(failure(invalid(
                Reason::InvalidConfig,
                "Source Associated prefix is empty",
            )));
        }
        prefix
            .nodes
            .iter()
            .map(|node| {
                let slot = plan.configured(&node.tag).ok_or_else(|| {
                    failure(invalid(Reason::Revoked, "Source Stream donor retired"))
                })?;
                if !node.slot.matches(&slot) || node.dial != slot.dial {
                    return Err(failure(invalid(
                        Reason::StalePlan,
                        "Source Stream donor generation changed",
                    )));
                }
                Ok(relay::node(
                    slot,
                    Some(node.tag.clone()),
                    node.endpoint.clone(),
                ))
            })
            .collect()
    }

    pub(crate) async fn build(
        &self,
        services: PacketPathExecutionServices,
    ) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
        if !services.has_packet_driver_caller() && !services.has_business_invocation() {
            return Err(failure(invalid(
                Reason::StalePlan,
                "standard Packet path has no original Source or Business caller",
            )));
        }
        let execution = services.tcp();
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| failure(crate::component::instance::retained(source.clone())))?;
        let generation = NetworkGeneration(execution.upstream().network_generation());
        let role = match &self.root {
            Root::Datagram(role) | Root::Associated { role, .. } => role,
        };
        let selected = role.selected.slot().map_err(failure)?;
        let tag = role.descriptor.tag.as_deref().ok_or_else(|| {
            failure(invalid(
                Reason::StalePlan,
                "Source root lost its configured tag",
            ))
        })?;
        let current = plan
            .configured(tag)
            .ok_or_else(|| failure(invalid(Reason::Revoked, "Source root retired")))?;
        if !role.selected.matches(&current) {
            return Err(failure(invalid(
                Reason::Revoked,
                "Source root actual generation changed",
            )));
        }
        let nodes = match &self.root {
            Root::Datagram(_) => Vec::new(),
            Root::Associated { prefix, .. } => Self::nodes(plan, prefix)?,
        };
        let binding = UpstreamBinding {
            prefix: nodes.iter().map(|node| node.slot.role_id()).collect(),
            dial: selected.dial.clone(),
            network_generation: generation,
        };
        if binding.prefix != self.binding.prefix
            || binding.dial != self.binding.dial
            || binding.network_generation != self.binding.network_generation
        {
            return Err(failure(invalid(
                Reason::StalePlan,
                "Source path dependency or network generation changed",
            )));
        }
        // The original root sends to the first tail endpoint. The existing
        // codecs wrap subsequent destinations; they never open another wire.
        let services = services.with_outbound_io(tag);
        let services = match self.formatters.first() {
            Some(source) => {
                let endpoint = source.descriptor().endpoint();
                services.with_packet_target(Destination::new(endpoint.target(), endpoint.port()))
            }
            None => services,
        };
        let business = if !services.has_packet_driver_caller() {
            Some(
                crate::protocol_registry::packet_memory::business::BusinessMemory::from_services(
                    &services.tcp(),
                )?,
            )
        } else {
            None
        };
        let mut path = match &self.root {
            Root::Datagram(role) => role.open(services.clone()).await?,
            Root::Associated { role, .. } => {
                if services.has_packet_driver_caller() {
                    StandardPacketPath::open_source_associated(
                        &services,
                        current,
                        nodes,
                        binding,
                        role.descriptor.clone(),
                    )
                    .await?
                } else {
                    super::business::open_selected_associated(
                        &services,
                        current,
                        nodes,
                        binding,
                        &role.descriptor,
                    )
                    .await?
                }
            }
        };
        for (index, source) in self.formatters.iter().enumerate() {
            path = crate::runtime::udp_flow::packet_path_chain::carriers::observed::wrap(
                path,
                services
                    .tcp()
                    .outbound_io(&source.descriptor().tag, zero_api::TrafficPlane::Outer),
            );
            path = crate::runtime::udp_flow::packet_path_chain::carriers::encoded::wrap_with_memory(
                path,
                source.clone(),
                services.packet_codec_receive(index)?,
                business.clone(),
            );
        }
        Ok(path)
    }
}

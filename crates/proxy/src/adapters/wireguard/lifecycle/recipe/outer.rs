//! Immutable existing leaf/flat-Relay order, over actual same-pass callees.
//! No protocol work, IO, Snapshot owner or synthetic component is created here.
use super::*;
#[derive(Clone)]
pub(in crate::adapters::wireguard::lifecycle) struct OuterPathRecipe {
    pub(super) tag: String,
    pub(super) network: std::sync::Weak<crate::component::background::BackgroundNetwork>,
    pub(super) ordered: Arc<[(String, ComponentId)]>,
    pub(super) projections: Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]>,
    pub(super) formatters: Arc<[Option<crate::runtime::udp_flow::packet_path::UdpDatagramSource>]>,
}
impl OuterPathRecipe {
    pub(in crate::adapters::wireguard::lifecycle) fn prepare(
        outbound: &OutboundConfig,
        resources: &mut crate::component::ResourcePreparation<'_>,
        registry: &crate::protocol_registry::ProtocolRegistry,
        config: &zero_config::RuntimeConfig,
        network: Option<&Arc<crate::component::background::BackgroundNetwork>>,
        materials: &mut std::collections::HashMap<
            String,
            Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]>,
        >,
    ) -> Result<Option<Self>, EngineError> {
        let OutboundProtocolConfig::Wireguard {
            outer_udp_proxy: Some(tag),
            ..
        } = &outbound.protocol
        else {
            return Ok(None);
        };
        if !resources
            .candidate()
            .is_some_and(|actual| std::ptr::eq(actual, config))
        {
            return Err(invalid("outer cold path has no same actual candidate"));
        }
        let names = if config.outbounds.iter().any(|outbound| outbound.tag == *tag) {
            vec![tag.clone()]
        } else {
            let group = config
                .outbound_groups
                .iter()
                .find(|group| group.tag == *tag)
                .ok_or_else(|| invalid("existing outer path is absent"))?;
            let zero_config::OutboundGroupKind::Relay { proxies } = &group.group else {
                return Err(invalid("existing outer path requires a flat Relay"));
            };
            if proxies.is_empty()
                || proxies.iter().any(|name| {
                    !config
                        .outbounds
                        .iter()
                        .any(|outbound| outbound.tag == *name)
                })
            {
                return Err(invalid(
                    "existing outer Relay requires actual named members",
                ));
            }
            proxies.clone()
        };
        let mut ordered = Vec::with_capacity(names.len());
        for name in &names {
            // Catalog memo consumes this exact immutable recipe later; no
            // second material decode and no new component ID is permitted.
            let id = resources
                .prepare_named_component(&name)
                .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            ordered.push((name.clone(), id));
        }
        let projections = match materials.get(tag) {
            Some(actual) => actual.clone(),
            None => {
                let mut values = Vec::with_capacity(names.len());
                for name in &names {
                    let index = config
                        .outbounds
                        .iter()
                        .position(|outbound| outbound.tag == *name)
                        .ok_or_else(|| invalid("actual packet path member is absent"))?;
                    let accepted = resources
                        .prepared_named_packet_path(name)
                        .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
                    let projection = match accepted {
                        Some(actual) => {
                            use sha2::Digest;
                            let mut key = sha2::Sha256::new();
                            key.update(b"accepted.packet.path.material.v1");
                            key.update((name.len() as u64).to_le_bytes());
                            key.update(name.as_bytes());
                            key.update(actual.recipe_identity());
                            let server = match &actual.endpoint.address {
                                zero_core::Address::Domain(name) => name.clone(),
                                zero_core::Address::Ipv4(bytes) => {
                                    std::net::Ipv4Addr::from(*bytes).to_string()
                                }
                                zero_core::Address::Ipv6(bytes) => {
                                    std::net::Ipv6Addr::from(*bytes).to_string()
                                }
                            };
                            Some(crate::protocol_registry::ColdPacketPathProjection {
                                descriptor: crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptor {
                                    tag: Some(name.clone()), server, port: actual.endpoint.port,
                                    cache_key: format!("accepted-packet:{:x}",key.finalize()),
                                },
                                associated: actual.associated,
                                formatter: None,
                            })
                        }
                        None => registry.prepare_cold_packet_path_projection(
                            config,
                            zero_engine::ResolvedLeafOutbound::Proxy {
                                identity: zero_engine::OutboundIdentity::from_config_index(index),
                            },
                        )?,
                    };
                    values.push(projection);
                }
                let values: Arc<[_]> = values.into();
                materials.insert(tag.clone(), values.clone());
                values
            }
        };
        // The first actual root is opened through its standard Datagram role;
        // only subsequent formatter layers use this same association family.
        let formatters = projections
            .iter()
            .enumerate()
            .map(|(position, value)| {
                (position != 0)
                    .then(|| value.as_ref().and_then(|value| value.formatter.clone()))
                    .flatten()
            })
            .collect::<Vec<_>>()
            .into();
        Ok(Some(Self {
            network: Arc::downgrade(network.ok_or_else(|| {
                invalid("actual Source outer path has no original Host services")
            })?),
            tag: tag.clone(),
            ordered: ordered.into(),
            formatters,
            projections,
        }))
    }
    pub(in crate::adapters::wireguard::lifecycle) fn dependencies(
        &self,
        resources: &crate::component::ResourcePreparation<'_>,
    ) -> Vec<(ComponentId, Option<ComponentInstanceId>)> {
        let mut ids = std::collections::BTreeSet::new();
        self.ordered
            .iter()
            .filter_map(|(_, id)| {
                ids.insert(*id)
                    .then(|| (*id, resources.reusable_resource_instance(*id)))
            })
            .collect()
    }
    pub(super) fn hash(&self, hash: &mut impl sha2::Digest) {
        hash.update(b"actual.ordered.outer.path.v1");
        hash.update((self.tag.len() as u64).to_le_bytes());
        hash.update(self.tag.as_bytes());
        for (name, id) in self.ordered.iter() {
            hash.update((name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
            hash.update(id.0.to_le_bytes());
        }
    }
}

impl OuterPathRecipe {
    /// Same last-Base/Associated suffix as PreparedDatagramRelayCarrier::build.
    /// A nonformatter remains the existing associated/native root boundary;
    /// this metadata never grants capability to an unsupported operation.
    pub(in crate::adapters::wireguard) fn supplemental_memory(
        &self,
        peers: usize,
    ) -> Result<zero_platform_tokio::contract::component::PacketSupplementalMemory, EngineError>
    {
        use std::num::NonZeroUsize;
        let start = self
            .formatters
            .iter()
            .rposition(Option::is_none)
            .map_or(0, |index| index + 1);
        let mut per_peer = vec![NonZeroUsize::new(65536).unwrap()]; // original actual proxy reader
        for source in self.formatters[start..].iter().flatten() {
            per_peer.push(NonZeroUsize::new(65536).unwrap()); // actual codec receive scratch
            let lengths = source
                .codec
                .decode_buffer_lengths(65536)
                .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            if lengths.is_empty() || lengths.len() > 2 {
                return Err(invalid("actual codec has an invalid fixed workspace count"));
            }
            for length in lengths {
                per_peer.push(
                    NonZeroUsize::new(length)
                        .ok_or_else(|| invalid("actual codec has zero-sized workspace"))?,
                );
            }
        }
        peers
            .checked_mul(per_peer.len())
            .ok_or_else(|| invalid("actual Source codec workspace count overflow"))?;
        Ok(
            zero_platform_tokio::contract::component::PacketSupplementalMemory {
                receive_workspaces: (0..peers).flat_map(|_| per_peer.iter().copied()).collect(),
                transmit_copy: NonZeroUsize::new(65536),
                transmit_target: NonZeroUsize::new(255),
            },
        )
    }
}

impl OuterPathRecipe {
    pub(in crate::adapters::wireguard) fn codec_receive_layouts(
        &self,
    ) -> Result<Arc<[Arc<[std::num::NonZeroUsize]>]>, EngineError> {
        let start = self
            .formatters
            .iter()
            .rposition(Option::is_none)
            .map_or(0, |index| index + 1);
        self.formatters[start..]
            .iter()
            .flatten()
            .map(|source| {
                let lengths = source
                    .codec
                    .decode_buffer_lengths(65536)
                    .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
                if lengths.is_empty() || lengths.len() > 2 {
                    return Err(invalid("actual fixed codec workspace count changed"));
                }
                lengths
                    .into_iter()
                    .map(|length| {
                        std::num::NonZeroUsize::new(length)
                            .ok_or_else(|| invalid("zero-sized actual codec workspace"))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Arc::from)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Arc::from)
    }
}

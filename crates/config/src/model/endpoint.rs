//! Resource configuration and neutral bindings for registered L3 protocols.

use serde::{Deserialize, Serialize};
use zero_api::EndpointDirections;

use super::{
    InboundConfig, InboundProtocolConfig, ListenConfig, OutboundConfig, OutboundProtocolConfig,
    RuntimeConfig, UdpPolicyConfig, WireguardInboundPeerConfig, WireguardPeerConfig,
    WireguardSecret,
};
use crate::ConfigError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    pub tag: String,
    #[serde(default = "enabled")]
    pub enabled: bool,
    #[serde(default = "EndpointDirections::outbound_only")]
    pub directions: EndpointDirections,
    #[serde(default)]
    pub listen: Option<ListenConfig>,
    pub protocol: EndpointProtocolConfig,
}

const fn enabled() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum EndpointProtocolConfig {
    #[serde(rename = "wireguard")]
    Wireguard {
        private_key: WireguardSecret,
        addresses: Vec<String>,
        #[serde(default = "default_mtu")]
        mtu: u16,
        peers: Vec<WireguardPeerConfig>,
        #[serde(default)]
        outer_udp_proxy: Option<String>,
    },
}

const fn default_mtu() -> u16 {
    ::wireguard::validation::DEFAULT_MTU
}

/// Protocol-independent ownership facts consumed by engine and orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointBindingConfig {
    pub endpoint_id: String,
    pub tag: String,
    pub protocol: String,
    pub inbound_tags: Vec<String>,
    pub outbound_tags: Vec<String>,
    pub enabled: bool,
    pub directions: EndpointDirections,
    pub supported_directions: EndpointDirections,
    pub canonical: bool,
}

impl EndpointConfig {
    pub fn endpoint_id(&self) -> String {
        format!("endpoint:{}", self.tag)
    }

    pub fn inbound_tag(&self) -> String {
        format!("endpoint/{}", self.tag)
    }

    pub fn outbound(&self) -> OutboundConfig {
        match &self.protocol {
            EndpointProtocolConfig::Wireguard {
                private_key,
                addresses,
                mtu,
                peers,
                outer_udp_proxy,
            } => OutboundConfig {
                tag: self.tag.clone(),
                protocol: OutboundProtocolConfig::Wireguard {
                    private_key: private_key.clone(),
                    addresses: addresses.clone(),
                    mtu: *mtu,
                    peers: peers.clone(),
                    inbound_tag: self.listen.as_ref().map(|_| self.inbound_tag()),
                    outer_udp_proxy: outer_udp_proxy.clone(),
                },
                udp: UdpPolicyConfig::default(),
            },
        }
    }

    pub fn inbound(&self) -> Option<InboundConfig> {
        let listen = self.listen.clone()?;
        match &self.protocol {
            EndpointProtocolConfig::Wireguard {
                private_key,
                mtu,
                peers,
                ..
            } => Some(InboundConfig {
                tag: self.inbound_tag(),
                listen,
                protocol: InboundProtocolConfig::Wireguard {
                    private_key: private_key.clone(),
                    mtu: *mtu,
                    peers: peers
                        .iter()
                        .map(|peer| WireguardInboundPeerConfig {
                            public_key: peer.public_key.clone(),
                            pre_shared_key: peer.pre_shared_key.clone(),
                            allowed_ips: peer.allowed_ips.clone(),
                            keepalive_secs: peer.keepalive_secs,
                            reserved: peer.reserved.clone(),
                        })
                        .collect(),
                },
                udp: UdpPolicyConfig::default(),
                idle_timeout_secs: None,
            }),
        }
    }

    pub fn binding(&self) -> EndpointBindingConfig {
        EndpointBindingConfig {
            endpoint_id: self.endpoint_id(),
            tag: self.tag.clone(),
            protocol: "wireguard".into(),
            inbound_tags: self
                .listen
                .as_ref()
                .map(|_| self.inbound_tag())
                .into_iter()
                .collect(),
            outbound_tags: vec![self.tag.clone()],
            enabled: self.enabled,
            directions: self.directions,
            supported_directions: EndpointDirections {
                inbound: self.listen.is_some(),
                outbound: true,
            },
            canonical: true,
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn validate_endpoint_projection_collisions(&self) -> Result<(), ConfigError> {
        for endpoint in &self.endpoints {
            if self
                .outbounds
                .iter()
                .any(|outbound| outbound.tag == endpoint.tag)
            {
                return Err(ConfigError::DuplicateRouteTargetTag {
                    tag: endpoint.tag.clone(),
                });
            }
            if endpoint.listen.is_some()
                && self
                    .inbounds
                    .iter()
                    .any(|inbound| inbound.tag == endpoint.inbound_tag())
            {
                return Err(ConfigError::DuplicateTag {
                    scope: "inbound",
                    tag: endpoint.inbound_tag(),
                });
            }
        }
        Ok(())
    }

    /// Materialize role projections once; serialization removes these views.
    pub fn materialize_endpoints(&mut self) -> Result<(), ConfigError> {
        let mut tags = std::collections::HashSet::new();
        for endpoint in &self.endpoints {
            if endpoint.tag.trim().is_empty() || !tags.insert(&endpoint.tag) {
                return Err(ConfigError::InvalidRuntime(
                    "empty or duplicate endpoint tag".into(),
                ));
            }
            if !endpoint
                .binding()
                .supported_directions
                .permits(endpoint.directions)
            {
                return Err(ConfigError::InvalidRuntime(format!(
                    "endpoint `{}` inbound direction requires a listen binding",
                    endpoint.tag
                )));
            }
            let outbound = endpoint.outbound();
            match self
                .outbounds
                .iter()
                .find(|current| current.tag == outbound.tag)
            {
                Some(current) if current != &outbound => {
                    return Err(ConfigError::DuplicateRouteTargetTag { tag: outbound.tag })
                }
                Some(_) => {}
                None => self.outbounds.push(outbound),
            }
            if let Some(inbound) = endpoint.inbound() {
                match self
                    .inbounds
                    .iter()
                    .find(|current| current.tag == inbound.tag)
                {
                    Some(current) if current != &inbound => {
                        return Err(ConfigError::DuplicateTag {
                            scope: "inbound",
                            tag: inbound.tag,
                        })
                    }
                    Some(_) => {}
                    None => self.inbounds.push(inbound),
                }
            }
        }
        Ok(())
    }

    pub fn endpoint_bindings(&self) -> Vec<EndpointBindingConfig> {
        let mut bindings: Vec<_> = self.endpoints.iter().map(EndpointConfig::binding).collect();
        let mut claimed_inbounds: std::collections::HashSet<String> = bindings
            .iter()
            .flat_map(|binding| binding.inbound_tags.iter().cloned())
            .collect();
        for outbound in &self.outbounds {
            if bindings
                .iter()
                .any(|binding| binding.outbound_tags.contains(&outbound.tag))
            {
                continue;
            }
            let OutboundProtocolConfig::Wireguard { inbound_tag, .. } = &outbound.protocol else {
                continue;
            };
            let endpoint_id = match inbound_tag {
                Some(tag) => {
                    claimed_inbounds.insert(tag.clone());
                    format!("legacy:inbound:{tag}")
                }
                None => format!("legacy:outbound:{}", outbound.tag),
            };
            bindings.push(EndpointBindingConfig {
                endpoint_id,
                tag: outbound.tag.clone(),
                protocol: outbound.protocol.protocol_name().into(),
                inbound_tags: inbound_tag.iter().cloned().collect(),
                outbound_tags: vec![outbound.tag.clone()],
                enabled: true,
                directions: EndpointDirections {
                    inbound: inbound_tag.is_some(),
                    outbound: true,
                },
                supported_directions: EndpointDirections {
                    inbound: inbound_tag.is_some(),
                    outbound: true,
                },
                canonical: false,
            });
        }
        for inbound in &self.inbounds {
            if claimed_inbounds.contains(&inbound.tag)
                || !matches!(inbound.protocol, InboundProtocolConfig::Wireguard { .. })
            {
                continue;
            }
            bindings.push(EndpointBindingConfig {
                endpoint_id: format!("legacy:inbound:{}", inbound.tag),
                tag: inbound.tag.clone(),
                protocol: inbound.protocol.protocol_name().into(),
                inbound_tags: vec![inbound.tag.clone()],
                outbound_tags: Vec::new(),
                enabled: true,
                directions: EndpointDirections {
                    inbound: true,
                    outbound: false,
                },
                supported_directions: EndpointDirections {
                    inbound: true,
                    outbound: false,
                },
                canonical: false,
            });
        }
        bindings.sort_by(|a, b| a.endpoint_id.cmp(&b.endpoint_id));
        bindings
    }
}

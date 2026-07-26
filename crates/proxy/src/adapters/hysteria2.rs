use async_trait::async_trait;

#[cfg(feature = "hysteria2")]
use ::hysteria2::inbound::Hysteria2InboundProfileStore;
use ::hysteria2::transport::{
    Hysteria2AuthenticatedInboundProfile, Hysteria2InboundBindOptionsRef, Hysteria2InboundBindPlan,
    Hysteria2InboundOptionsRef, Hysteria2InboundUserRef, Hysteria2OutboundOptionsRef,
    Hysteria2TransportLeaf,
};
use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig};
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    inbound_listen_addr, BoundInbound, InboundListenerCapability, ManagedUdpHandlerProvider,
    OutboundLeafClaim, OutboundLeafInput, TcpOutboundCapability, UdpFlowCapability,
    UdpPacketPathCapability,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::udp_flow::managed::ManagedDatagramFlowHandler;

#[cfg(feature = "hysteria2")]
mod inbound;
#[cfg(feature = "hysteria2")]
mod tcp;
#[cfg(feature = "hysteria2")]
pub(crate) mod udp;

#[cfg(feature = "hysteria2")]
#[derive(Debug, Default)]
pub(crate) struct Hysteria2Adapter {
    inbound_profiles: Hysteria2InboundProfileStore,
}

#[cfg(feature = "hysteria2")]
fn inbound_user_refs<'a>(
    password: &'a str,
    users: &'a [zero_config::Hysteria2UserConfig],
) -> Vec<Hysteria2InboundUserRef<'a>> {
    if users.is_empty() {
        return (!password.is_empty())
            .then_some(Hysteria2InboundUserRef {
                password,
                principal_key: None,
                up_bps: None,
                down_bps: None,
                device_limit: None,
                quota_remaining_bytes: None,
                policy_revision: None,
            })
            .into_iter()
            .collect();
    }
    users
        .iter()
        .map(|user| Hysteria2InboundUserRef {
            password: user.password.as_str(),
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

fn transport_leaf(tag: &str, protocol: &OutboundProtocolConfig) -> Option<Hysteria2TransportLeaf> {
    let OutboundProtocolConfig::Hysteria2 {
        server,
        port,
        password,
        client_fingerprint,
        ..
    } = protocol
    else {
        return None;
    };
    Some(Hysteria2TransportLeaf::from_options_refs(
        tag,
        server,
        *port,
        Hysteria2OutboundOptionsRef {
            password,
            client_fingerprint: client_fingerprint.as_deref(),
        },
    ))
}

#[cfg(feature = "hysteria2")]
impl NamedProtocolAdapter for Hysteria2Adapter {
    const PROTOCOL_NAME: &'static str = "hysteria2";
    const FEATURE_NAME: &'static str = "hysteria2";

    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Hysteria2 {
                password, users, ..
            } = &inbound.protocol
            else {
                continue;
            };
            let users = inbound_user_refs(password, users);
            self.inbound_profiles.replace(&inbound.tag, &users);
        }
    }
}

#[cfg(feature = "hysteria2")]
impl Hysteria2Adapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy { outbound, .. } = input else {
            return None;
        };
        let leaf = transport_leaf(outbound.tag(), &outbound.protocol)?;
        let tcp = self.claim_tcp_outbound_leaf_impl(leaf.clone());
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::TransportSession,
            tcp,
            udp: Some(self.claim_udp_flow_leaf_impl(leaf.clone())),
            packet_path: self.claim_udp_packet_path_leaf_impl(leaf),
        })
    }
}

#[cfg(feature = "hysteria2")]
impl UdpPacketPathCapability for Hysteria2Adapter {}

#[cfg(feature = "hysteria2")]
impl UdpFlowCapability for Hysteria2Adapter {}

#[cfg(feature = "hysteria2")]
impl ManagedUdpHandlerProvider for Hysteria2Adapter {
    fn managed_datagram_udp_handler(&self) -> Option<Box<dyn ManagedDatagramFlowHandler>> {
        Some(udp::managed_datagram_handler())
    }
}

#[cfg(feature = "hysteria2")]
#[async_trait]
impl InboundListenerCapability for Hysteria2Adapter {
    async fn bind_inbound(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<BoundInbound, EngineError> {
        let InboundProtocolConfig::Hysteria2 {
            cert_path,
            key_path,
            ..
        } = &inbound.protocol
        else {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "hysteria2 inbound bind received non-hysteria2 inbound config",
            )));
        };
        let plan = Hysteria2InboundBindPlan::from_options_refs(
            source_dir,
            Hysteria2InboundBindOptionsRef {
                cert_path: cert_path.as_deref(),
                key_path: key_path.as_deref(),
            },
        );
        let endpoint = plan.bind(&inbound_listen_addr(inbound)).await?;
        Ok(BoundInbound::Quic(endpoint))
    }

    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let profile = match &inbound.protocol {
            InboundProtocolConfig::Hysteria2 {
                password, users, ..
            } => {
                let users = inbound_user_refs(password, users);
                let profile = self.inbound_profiles.replace(&inbound.tag, &users);
                Hysteria2AuthenticatedInboundProfile::from_options_refs(
                    Hysteria2InboundOptionsRef {
                        users: users.iter().copied(),
                    },
                )
                .with_profile(profile)
            }
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "hysteria2 inbound listener received non-hysteria2 inbound config",
                )));
            }
        };
        Ok(inbound::prepare(profile))
    }
}

#[cfg(feature = "hysteria2")]
impl TcpOutboundCapability for Hysteria2Adapter {}

#[cfg(feature = "hysteria2")]
impl ProtocolMetadata for Hysteria2Adapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::hysteria2::Hysteria2Protocol.descriptor()
    }
}

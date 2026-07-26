use ::shadowsocks::transport::{
    ShadowsocksInboundBindings, ShadowsocksInboundOptionsRef, ShadowsocksInboundUserRef,
    ShadowsocksOutboundOptionsRef, ShadowsocksTransportLeaf,
};
#[cfg(feature = "shadowsocks")]
use ::shadowsocks::ShadowsocksInboundProfileStore;
use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig};
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    InboundListenerCapability, ManagedUdpHandlerProvider, OutboundLeafClaim, OutboundLeafInput,
    TcpOutboundCapability, UdpFlowCapability, UdpPacketPathCapability,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::udp_flow::managed::ManagedDatagramFlowHandler;

#[cfg(feature = "shadowsocks")]
mod inbound;
#[cfg(feature = "shadowsocks")]
mod tcp;
#[cfg(feature = "shadowsocks")]
pub(crate) mod udp;

#[cfg(feature = "shadowsocks")]
#[derive(Debug, Default)]
pub(crate) struct ShadowsocksAdapter {
    inbound_profiles: ShadowsocksInboundProfileStore,
}

#[cfg(feature = "shadowsocks")]
fn inbound_user_refs<'a>(
    password: &'a str,
    users: &'a [zero_config::ShadowsocksUserConfig],
) -> Vec<ShadowsocksInboundUserRef<'a>> {
    if users.is_empty() {
        return (!password.is_empty())
            .then_some(ShadowsocksInboundUserRef {
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
        .map(|user| ShadowsocksInboundUserRef {
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

#[cfg(feature = "shadowsocks")]
fn transport_leaf(
    tag: &str,
    protocol: &OutboundProtocolConfig,
) -> Option<ShadowsocksTransportLeaf> {
    let OutboundProtocolConfig::Shadowsocks {
        server,
        port,
        password,
        cipher,
    } = protocol
    else {
        return None;
    };
    Some(ShadowsocksTransportLeaf::from_options_refs(
        tag,
        server,
        *port,
        ShadowsocksOutboundOptionsRef { cipher, password },
    ))
}

#[cfg(feature = "shadowsocks")]
impl NamedProtocolAdapter for ShadowsocksAdapter {
    const PROTOCOL_NAME: &'static str = "shadowsocks";
    const FEATURE_NAME: &'static str = "shadowsocks";

    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Shadowsocks {
                password,
                identity_password,
                users,
                cipher,
                ..
            } = &inbound.protocol
            else {
                continue;
            };
            let users = inbound_user_refs(password, users);
            let _ = self.inbound_profiles.replace_with_identity(
                &inbound.tag,
                cipher,
                identity_password.as_deref(),
                &users,
            );
        }
    }
}

#[cfg(feature = "shadowsocks")]
impl ShadowsocksAdapter {
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
            tcp_path: TcpPathCategory::Session,
            tcp,
            udp: Some(self.claim_udp_flow_leaf_impl(leaf.clone())),
            packet_path: self.claim_udp_packet_path_leaf_impl(leaf),
        })
    }
}

#[cfg(feature = "shadowsocks")]
impl UdpPacketPathCapability for ShadowsocksAdapter {}

#[cfg(feature = "shadowsocks")]
impl UdpFlowCapability for ShadowsocksAdapter {}

#[cfg(feature = "shadowsocks")]
impl ManagedUdpHandlerProvider for ShadowsocksAdapter {
    fn managed_datagram_udp_handler(&self) -> Option<Box<dyn ManagedDatagramFlowHandler>> {
        Some(udp::managed_datagram_handler())
    }
}

#[cfg(feature = "shadowsocks")]
impl InboundListenerCapability for ShadowsocksAdapter {
    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let bindings = match &inbound.protocol {
            InboundProtocolConfig::Shadowsocks {
                password,
                identity_password,
                users,
                cipher,
                ..
            } => {
                let users = inbound_user_refs(password, users);
                let profile = self
                    .inbound_profiles
                    .replace_with_identity(
                        &inbound.tag,
                        cipher,
                        identity_password.as_deref(),
                        &users,
                    )
                    .map_err(|error| {
                        EngineError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            error.to_string(),
                        ))
                    })?;
                ShadowsocksInboundBindings::from_options_refs(ShadowsocksInboundOptionsRef {
                    cipher,
                    identity_password: identity_password.as_deref(),
                    users: users.iter().copied(),
                })?
                .with_profile(profile)
            }
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "shadowsocks inbound listener received non-shadowsocks inbound config",
                )));
            }
        };
        Ok(inbound::prepare(
            inbound.listen.address,
            inbound.listen.port,
            bindings,
        ))
    }
}

#[cfg(feature = "shadowsocks")]
impl TcpOutboundCapability for ShadowsocksAdapter {}

#[cfg(feature = "shadowsocks")]
impl ProtocolMetadata for ShadowsocksAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::shadowsocks::ShadowsocksProtocol.descriptor()
    }
}

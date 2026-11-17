#[cfg(feature = "trojan")]
mod options;
#[cfg(feature = "trojan")]
use options::outbound_options;
#[cfg(feature = "trojan")]
pub(crate) mod component;
#[cfg(feature = "trojan")]
pub(crate) mod inbound;
#[cfg(feature = "trojan")]
use ::trojan::inbound::TrojanInboundProfileStore;
use ::trojan::transport::{
    TrojanInboundListenerRequest, TrojanInboundOptionsRef, TrojanInboundUserRef,
    TrojanOutboundBuildOptionsRef, TrojanOutboundOptionsRef,
};
#[cfg(feature = "trojan")]
pub(crate) use component::{component_options, native_input, LEGACY_CONNECT_STAGE};
#[cfg(feature = "trojan")]
use zero_config::InboundConfig;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;

#[cfg(feature = "trojan")]
#[derive(Debug, Default)]
pub(crate) struct TrojanAdapter {
    pub(crate) browser: zero_transport::browser_dialer::registry::Registry,
    pub(crate) native_node_dns: bool,
    inbound_profiles: TrojanInboundProfileStore,
    mux_pool: ::trojan::mux::TrojanMuxConnectionPool,
}

#[cfg(feature = "trojan")]
fn inbound_user_refs<'a>(
    password: &'a str,
    users: &'a [zero_config::TrojanUserConfig],
) -> Vec<TrojanInboundUserRef<'a>> {
    if users.is_empty() {
        return (!password.is_empty())
            .then_some(TrojanInboundUserRef {
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
        .map(|user| TrojanInboundUserRef {
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

#[cfg(feature = "trojan")]
impl NamedProtocolAdapter for TrojanAdapter {
    const PROTOCOL_NAME: &'static str = "trojan";
    const FEATURE_NAME: &'static str = "trojan";

    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.mux_pool.evict_all();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Trojan {
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

#[cfg(feature = "trojan")]
impl ProtocolMetadata for TrojanAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::trojan::metadata::TrojanProtocol.descriptor()
    }
}

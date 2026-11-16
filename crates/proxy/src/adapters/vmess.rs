#[cfg(feature = "vmess")]
pub(crate) mod component;
#[cfg(feature = "vmess")]
pub(crate) mod inbound;
use ::vmess::transport::{
    VmessInboundListenerRequest, VmessInboundOptionsRef, VmessInboundUserRef,
    VmessOutboundBuildOptionsRef, VmessOutboundOptionsRef, VmessTransportRuntime,
};
#[cfg(feature = "vmess")]
pub(crate) use component::LEGACY_CONNECT_STAGE;
#[cfg(feature = "vmess")]
use zero_config::InboundConfig;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
#[cfg(feature = "vmess")]
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;

#[cfg(feature = "vmess")]
#[derive(Debug, Default)]
pub(crate) struct VmessAdapter {
    pub(crate) browser: zero_transport::browser_dialer::registry::Registry,
    runtime: VmessTransportRuntime,
    pub(crate) native_node_dns: bool,
}

#[cfg(feature = "vmess")]
fn inbound_user_refs(users: &[zero_config::VmessUserConfig]) -> Vec<VmessInboundUserRef<'_>> {
    users
        .iter()
        .map(|user| VmessInboundUserRef {
            id: user.id.as_str(),
            cipher: user.cipher.as_str(),
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

#[cfg(feature = "vmess")]
impl NamedProtocolAdapter for VmessAdapter {
    const PROTOCOL_NAME: &'static str = "vmess";
    const FEATURE_NAME: &'static str = "vmess";

    fn validate_runtime_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), EngineError> {
        let grpc = component::grpc::has_grpc_nodes(config);
        let streams = component::grpc::whole_streams_selected(config, self.native_node_dns);
        self.runtime
            .validate_grpc_stream_candidate(streams, grpc && !streams)
            .map_err(EngineError::Core)
    }
    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.runtime.on_config_reloaded();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Vmess { users, .. } = &inbound.protocol else {
                continue;
            };
            let users = inbound_user_refs(users);
            if let Err(error) = self.runtime.replace_inbound_profile(&inbound.tag, &users) {
                tracing::error!(
                    inbound_tag = %inbound.tag,
                    %error,
                    "validated VMess users could not be applied to the live profile"
                );
            }
        }
    }
}

#[cfg(feature = "vmess")]
impl ProtocolMetadata for VmessAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::vmess::metadata::VmessProtocol.descriptor()
    }
}

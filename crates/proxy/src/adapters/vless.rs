#[cfg(feature = "vless")]
pub(crate) mod component;
#[cfg(feature = "vless")]
pub(crate) use component::{component_options, native_input, LEGACY_CONNECT_STAGE};
#[cfg(feature = "vless")]
mod reverse;
#[cfg(feature = "vless")]
use ::vless::transport::{
    VlessInboundBindPlan, VlessInboundListenerRequest, VlessInboundOptionsRef, VlessInboundUserRef,
    VlessOutboundBuildOptionsRef, VlessOutboundOptionsRef, VlessQuicBindOptionsRef,
    VlessQuicClientOptionsRef, VlessRealityClientOptionsRef, VlessRealityServerOptionsRef,
    VlessTransportRuntime,
};
#[cfg(feature = "vless")]
mod finalmask;
#[cfg(feature = "vless")]
mod hysteria;
#[cfg(feature = "vless")]
pub(crate) mod inbound;
#[cfg(feature = "vless")]
mod mkcp;
#[cfg(feature = "vless")]
mod xhttp;
#[cfg(feature = "vless")]
use zero_config::InboundConfig;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
#[cfg(feature = "vless")]
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;

#[cfg(feature = "vless")]
#[derive(Debug, Default)]
pub(crate) struct VlessAdapter {
    runtime: VlessTransportRuntime,
    pub(crate) native_node_dns: bool,
}

#[cfg(feature = "vless")]
fn inbound_user_refs(users: &[zero_config::VlessUserConfig]) -> Vec<VlessInboundUserRef<'_>> {
    users
        .iter()
        .map(|user| VlessInboundUserRef {
            reverse_tag: user.reverse_tag.as_deref(),
            id: user.id.as_str(),
            flow: user.flow.as_deref(),
            testseed: &user.testseed,
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        })
        .collect()
}

#[cfg(feature = "vless")]
impl NamedProtocolAdapter for VlessAdapter {
    const PROTOCOL_NAME: &'static str = "vless";
    const FEATURE_NAME: &'static str = "vless";

    fn validate_runtime_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), EngineError> {
        let nodes = component::grpc::has_nodes(config);
        let native = component::grpc::whole_selected(config, self.native_node_dns);
        self.runtime
            .validate_grpc_candidate(native, nodes && !native)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))
    }
    fn on_config_reloaded(&self, config: &zero_config::RuntimeConfig) {
        self.runtime.on_config_reloaded();
        for inbound in &config.inbounds {
            let InboundProtocolConfig::Vless { users, .. } = &inbound.protocol else {
                continue;
            };
            let users = inbound_user_refs(users);
            if let Err(error) = self.runtime.replace_inbound_profile(&inbound.tag, &users) {
                tracing::error!(
                    inbound_tag = %inbound.tag,
                    %error,
                    "validated VLESS users could not be applied to the live profile"
                );
            }
        }
    }
}

#[cfg(feature = "vless")]
impl ProtocolMetadata for VlessAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::vless::metadata::VlessProtocol.descriptor()
    }
}

mod reality;

#[cfg(feature = "vless")]
mod options;
#[cfg(feature = "vless")]
use options::outbound_options;

impl VlessAdapter {
    pub(crate) fn set_browser_registry(
        &mut self,
        registry: zero_transport::browser_dialer::registry::Registry,
    ) {
        self.runtime = VlessTransportRuntime::with_browser_registry(registry);
    }
}

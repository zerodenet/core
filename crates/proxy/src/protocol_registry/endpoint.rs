//! Narrow read-only resource observation registered independently of dispatch.

use zero_api::{EndpointCapabilities, EndpointHealthState, EndpointRuntimeState};
use zero_config::{EndpointBindingConfig, RuntimeConfig};

#[derive(Default)]
pub(crate) struct EndpointObservation {
    pub supported: EndpointCapabilities,
    pub state: EndpointRuntimeState,
    pub health: EndpointHealthState,
    pub generation: Option<u64>,
    /// Opaque identities of actual device components, never protocol keys.
    pub incarnations: Vec<u64>,
    pub details: Option<(String, u32, serde_json::Value)>,
}

pub(crate) trait EndpointObservationCapability: Send + Sync {
    fn observe_endpoint(
        &self,
        binding: &EndpointBindingConfig,
        config: &RuntimeConfig,
    ) -> Option<EndpointObservation>;
}

/// Opt in to the neutral acknowledged resource/configuration executor. The
/// owning listener and device capabilities must confirm revoked tasks ended.
pub(crate) trait EndpointControlCapability: Send + Sync {
    fn supports_endpoint_control(&self, binding: &EndpointBindingConfig) -> bool;

    fn live_direction_contraction(
        &self,
        _binding: &EndpointBindingConfig,
    ) -> zero_api::EndpointDirections {
        zero_api::EndpointDirections::default()
    }
}

use crate::protocol_registry::{
    EndpointObservation, EndpointObservationCapability, ProtocolRegistry,
};
use std::sync::Arc;
use zero_api::{EndpointCapabilities, EndpointDirections, EndpointRuntimeState};
use zero_config::{EndpointBindingConfig, RuntimeConfig};

struct TestEndpointObserver;

impl EndpointObservationCapability for TestEndpointObserver {
    fn observe_endpoint(
        &self,
        binding: &EndpointBindingConfig,
        _config: &RuntimeConfig,
    ) -> Option<EndpointObservation> {
        (binding.protocol == "test-l3").then(|| EndpointObservation {
            supported: EndpointCapabilities {
                directions: binding.supported_directions,
                packet: true,
                ..Default::default()
            },
            state: EndpointRuntimeState::Running,
            generation: Some(7),
            ..Default::default()
        })
    }
}

#[test]
fn endpoint_observers_register_without_protocol_named_dispatch() {
    let config =
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap();
    let binding = EndpointBindingConfig {
        endpoint_id: "endpoint:test".into(),
        tag: "test".into(),
        protocol: "test-l3".into(),
        inbound_tags: vec!["in".into()],
        outbound_tags: vec!["out".into()],
        enabled: true,
        directions: EndpointDirections {
            inbound: true,
            outbound: true,
        },
        supported_directions: EndpointDirections {
            inbound: true,
            outbound: true,
        },
        canonical: true,
    };
    let mut registry = ProtocolRegistry::default();
    assert!(registry.observe_endpoint(&binding, &config).is_none());
    registry.register_endpoint_observer(Arc::new(TestEndpointObserver));
    let observed = registry.observe_endpoint(&binding, &config).unwrap();
    assert_eq!(observed.state, EndpointRuntimeState::Running);
    assert_eq!(observed.generation, Some(7));
    assert!(observed.supported.packet);
}

use super::ProtocolInventory;
use zero_api::{
    EndpointDirections, EndpointOperationCapability, EndpointPersistence, EndpointRuntimeState,
    EndpointSnapshot,
};
use zero_config::RuntimeConfig;

impl ProtocolInventory {
    pub(crate) fn endpoint_control_available(&self) -> bool {
        self.registry.endpoint_control_available()
    }
    pub(crate) fn endpoint_control_supported(
        &self,
        binding: &zero_config::EndpointBindingConfig,
    ) -> bool {
        self.registry.endpoint_control_supported(binding)
    }

    pub(crate) fn observe_endpoint(
        &self,
        config: &RuntimeConfig,
        endpoint: &mut EndpointSnapshot,
    ) -> Option<crate::protocol_registry::EndpointObservation> {
        let binding = config
            .endpoint_bindings()
            .into_iter()
            .find(|binding| binding.endpoint_id == endpoint.endpoint_id)?;
        let observed = self.registry.observe_endpoint(&binding, config)?;
        endpoint.supported = observed.supported.clone();
        if self.endpoint_control_supported(&binding) {
            endpoint.supported.operations.extend(
                ["set_state", "set_directions", "restart", "clear_overrides"].map(str::to_owned),
            );
            for operation in ["set_state", "set_directions", "restart", "clear_overrides"] {
                let mut persistence = vec![EndpointPersistence::RuntimeOnly];
                if matches!(operation, "set_state" | "set_directions")
                    && endpoint.configuration.source_file.available
                {
                    persistence.push(EndpointPersistence::SourceFile);
                }
                endpoint.supported.operation_capabilities.insert(
                    operation.into(),
                    EndpointOperationCapability {
                        persistence,
                        preconditions: vec![
                            "expected_core_instance_id".into(),
                            "expected_intent_revision".into(),
                        ],
                        live_direction_contraction: matches!(
                            operation,
                            "set_directions" | "clear_overrides"
                        )
                        .then_some(EndpointDirections {
                            inbound: binding.supported_directions.inbound,
                            outbound: false,
                        }),
                    },
                );
            }
        }
        if !matches!(
            endpoint.state,
            EndpointRuntimeState::Starting
                | EndpointRuntimeState::Stopping
                | EndpointRuntimeState::Failed
        ) {
            endpoint.state = observed.state;
        }
        endpoint.health = observed.health;
        endpoint.generation = observed.generation.or(endpoint.generation);
        endpoint.effective = if endpoint.enabled && endpoint.state == EndpointRuntimeState::Running
        {
            EndpointDirections {
                inbound: endpoint.allowed.inbound && endpoint.supported.directions.inbound,
                outbound: endpoint.allowed.outbound && endpoint.supported.directions.outbound,
            }
        } else {
            EndpointDirections::default()
        };
        Some(observed)
    }
}

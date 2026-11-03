//! Domain projection of facts observed by the runtime-owned source writer.
use zero_api::EndpointSourceFileCapability;
impl super::super::Engine {
    pub fn config_source_write_generation(&self) -> u64 {
        self.config_source.observation().successful_writes
    }
    pub(crate) fn endpoint_source_file_capability(
        &self,
        canonical: bool,
    ) -> EndpointSourceFileCapability {
        let unavailable = if !canonical {
            Some("canonical_configuration_required")
        } else if self.config_path().is_none() {
            Some("source_path_unavailable")
        } else {
            None
        };
        if let Some(reason) = unavailable {
            return EndpointSourceFileCapability {
                reason: Some(reason.into()),
                ..Default::default()
            };
        }
        let fact = self.config_source.observation();
        EndpointSourceFileCapability {
            available: true,
            writable: fact.writable,
            writable_observed_at_unix_ms: fact.observed_at_unix_ms,
            reason: fact.reason,
        }
    }
}

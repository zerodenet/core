//! Source persistence observations; queries never probe by writing files.

use std::{io, path::Path};
use zero_api::EndpointSourceFileCapability;
use zero_config::RuntimeConfig;

#[derive(Debug, Default)]
pub(in crate::runtime) struct ConfigWriteFact {
    successful_writes: u64,
    writable: Option<bool>,
    observed_at_unix_ms: Option<u64>,
    reason: Option<String>,
}

impl super::super::Engine {
    /// Transaction bookkeeping for source rollback, not a client revision.
    pub fn config_source_write_generation(&self) -> u64 {
        self.config_write_fact
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .successful_writes
    }

    pub(crate) fn endpoint_source_file_capability(
        &self,
        canonical: bool,
    ) -> EndpointSourceFileCapability {
        let unavailable = if !canonical {
            Some("canonical_configuration_required")
        } else if self.config_path.is_none() {
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
        let fact = self
            .config_write_fact
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        EndpointSourceFileCapability {
            available: true,
            writable: fact.writable,
            writable_observed_at_unix_ms: fact.observed_at_unix_ms,
            reason: fact.reason.clone(),
        }
    }

    pub(super) fn persist_config_to_file(
        &self,
        path: &Path,
        config: &RuntimeConfig,
    ) -> io::Result<()> {
        let result = super::write_config_to_file(path, config);
        let mut fact = self
            .config_write_fact
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        fact.observed_at_unix_ms = Some(super::super::started_at_unix_ms());
        match &result {
            Ok(()) => {
                fact.successful_writes = fact.successful_writes.wrapping_add(1);
                fact.writable = Some(true);
                fact.reason = None;
            }
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                fact.writable = Some(false);
                fact.reason = Some("source_write_permission_denied".into());
            }
            Err(_) => {
                fact.writable = None;
                fact.reason = Some("source_write_failed".into());
            }
        }
        result
    }
}

use super::{
    intent::{EndpointIntents, Override},
    Engine,
};
use crate::EngineRuntimeSnapshot;
use std::sync::Arc;
use zero_api::{ApiError, ApiErrorCode, EndpointDirections};

/// A policy mutation; socket/task execution remains in Proxy.
#[derive(Debug, Clone, Copy)]
pub enum EndpointChange {
    Enabled(bool),
    Directions(EndpointDirections),
    ClearOverrides,
    Restart,
}

impl Engine {
    pub fn endpoint_source_file_available(&self) -> bool {
        self.config_path.is_some()
    }

    /// Build a candidate without publishing it. The caller stages it through
    /// the existing acknowledged configuration transaction and restores the
    /// previous snapshot on failure.
    pub fn prepare_endpoint_change(
        &self,
        current: &Arc<EngineRuntimeSnapshot>,
        id: &str,
        change: EndpointChange,
        persist: bool,
        expected_revision: Option<u64>,
    ) -> zero_api::ApiResult<Arc<EngineRuntimeSnapshot>> {
        let entry = current.endpoint_intents.entries.get(id).ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::NotFound,
                format!("endpoint `{id}` was not found"),
            )
        })?;
        if expected_revision.is_some_and(|expected| expected != entry.revision) {
            return Err(ApiError::new(
                ApiErrorCode::Conflict,
                "endpoint intent revision changed",
            ));
        }
        if let EndpointChange::Directions(directions) = change {
            if !entry.binding.supported_directions.permits(directions) {
                return Err(ApiError::new(
                    ApiErrorCode::InvalidArgument,
                    "endpoint direction has no configured executable role",
                ));
            }
        }
        if matches!(change, EndpointChange::Restart) && !entry.enabled() {
            return Err(ApiError::new(
                ApiErrorCode::InvalidArgument,
                "cannot restart a disabled endpoint",
            ));
        }
        let mut intents = (*current.endpoint_intents).clone();
        let mut config = current.config.clone();
        if persist {
            if !self.endpoint_source_file_available() {
                return Err(ApiError::new(
                    ApiErrorCode::Unsupported,
                    "source_file requires an engine configuration source path",
                ));
            }
            if !entry.binding.canonical {
                return Err(ApiError::new(
                    ApiErrorCode::Unsupported,
                    "source_file endpoint control requires canonical endpoints configuration",
                ));
            }
            let mut candidate = (*config).clone();
            let endpoint = candidate
                .endpoints
                .iter_mut()
                .find(|endpoint| endpoint.endpoint_id() == id)
                .expect("canonical endpoint binding");
            match change {
                EndpointChange::Enabled(enabled) => endpoint.enabled = enabled,
                EndpointChange::Directions(directions) => endpoint.directions = directions,
                _ => {
                    return Err(ApiError::new(
                        ApiErrorCode::InvalidArgument,
                        "operation does not persist configuration",
                    ))
                }
            }
            intents = EndpointIntents::for_config(&candidate, Some(&intents))?;
            intents.replace_override(id, Override::default(), false)?;
            config = Arc::new(candidate);
        } else {
            let mut overlay = entry.overlay.clone();
            match change {
                EndpointChange::Enabled(enabled) => overlay.enabled = Some(enabled),
                EndpointChange::Directions(directions) => overlay.directions = Some(directions),
                EndpointChange::ClearOverrides => overlay = Override::default(),
                EndpointChange::Restart => {}
            }
            intents.replace_override(id, overlay, matches!(change, EndpointChange::Restart))?;
        }
        if Arc::ptr_eq(&config, &current.config)
            && intents.entries.get(id).map(|entry| entry.revision) == Some(entry.revision)
        {
            return Ok(current.clone());
        }
        Ok(Arc::new(EngineRuntimeSnapshot {
            config_revision: Arc::new(std::sync::atomic::AtomicU64::new(current.config_revision())),
            config,
            endpoint_intents: Arc::new(intents),
            plan: current.plan.clone(),
            router: current.router.clone(),
            bypass: current.bypass.clone(),
            outbound_group_state: current.outbound_group_state.clone(),
        }))
    }
}

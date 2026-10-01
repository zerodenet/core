use super::{
    counter::{epoch, now},
    view::project,
    TrafficRegistry,
};
use zero_api::{ApiError, ApiErrorCode, StatsResetCommand, StatsResetSnapshot};

impl TrafficRegistry {
    pub fn reset(
        &self,
        request: &StatsResetCommand,
        core: &str,
        _revision: u64,
        operation_id: String,
        publish: impl FnOnce(&mut StatsResetSnapshot),
    ) -> zero_api::ApiResult<StatsResetSnapshot> {
        if request.expected_core_instance_id != core {
            return Err(precondition(
                "expected_core_instance_id".into(),
                "statistics core_instance_id changed",
            ));
        }
        if request.targets.is_empty() || request.targets.len() > 256 {
            return Err(ApiError::new(
                ApiErrorCode::InvalidArgument,
                "reset requires 1..256 explicit scopes",
            ));
        }
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let revision = self
            .config_revision
            .load(std::sync::atomic::Ordering::Relaxed);
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        let mut seen = std::collections::BTreeSet::new();
        for (index, target) in request.targets.iter().enumerate() {
            if !seen.insert(&target.scope) {
                return Err(ApiError::new(
                    ApiErrorCode::InvalidArgument,
                    "duplicate reset scope",
                ));
            }
            let entry = entries.get(&target.scope).ok_or_else(|| {
                ApiError::new(ApiErrorCode::NotFound, "statistics scope no longer exists")
            })?;
            let period = entry.period.lock().unwrap_or_else(|e| e.into_inner());
            if period.epoch != target.expected_stats_epoch {
                return Err(precondition(
                    format!("targets[{index}].expected_stats_epoch"),
                    "statistics epoch changed",
                ));
            }
            let generation = *entry.generation.lock().unwrap_or_else(|e| e.into_inner());
            if target.expected_generation != generation {
                return Err(precondition(
                    format!("targets[{index}].expected_generation"),
                    "resource generation changed or was not supplied",
                ));
            }
            if project(&target.scope, entry, &period, core, revision)
                .planes
                .iter()
                .all(|p| p.resettable_metrics.is_empty())
            {
                return Err(ApiError::new(
                    ApiErrorCode::Unsupported,
                    "scope has no resettable observed cumulative metrics",
                ));
            }
        }
        let mut snapshots = Vec::with_capacity(request.targets.len());
        for target in &request.targets {
            let entry = &entries[&target.scope];
            let mut period = entry.period.lock().unwrap_or_else(|e| e.into_inner());
            period.baseline = entry.capture();
            period.epoch = epoch();
            period.started = now();
            snapshots.push(project(&target.scope, entry, &period, core, revision));
        }
        let mut result = StatsResetSnapshot {
            operation_id,
            core_instance_id: core.into(),
            snapshots,
        };
        publish(&mut result);
        Ok(result)
    }
}

fn precondition(field: String, message: &str) -> ApiError {
    let mut error = ApiError::new(ApiErrorCode::Conflict, message);
    let field = format!("params.{field}");
    error.field_path = Some(field.clone());
    error.with_detail(zero_api::ErrorDetail::new(Some(field), message))
}

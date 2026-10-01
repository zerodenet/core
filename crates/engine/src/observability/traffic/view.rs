use super::{
    counter::{now, CounterSet, Period},
    TrafficRegistry,
};
use std::sync::atomic::Ordering;
use zero_api::{
    ApiError, ApiErrorCode, TrafficActivity, TrafficListQuery, TrafficListSnapshot, TrafficPlane,
    TrafficScope, TrafficSnapshot,
};

impl TrafficRegistry {
    pub fn snapshot(
        &self,
        scope: &TrafficScope,
        core: &str,
        _revision: u64,
    ) -> zero_api::ApiResult<TrafficSnapshot> {
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let revision = self.config_revision.load(Ordering::Relaxed);
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        let entry = entries.get(scope).ok_or_else(|| {
            ApiError::new(ApiErrorCode::NotFound, "statistics scope no longer exists")
        })?;
        let period = entry.period.lock().unwrap_or_else(|e| e.into_inner());
        Ok(project(scope, entry, &period, core, revision))
    }
    pub fn list(
        &self,
        query: &TrafficListQuery,
        core: &str,
        _revision: u64,
    ) -> zero_api::ApiResult<TrafficListSnapshot> {
        if query.scopes.len() > 256 {
            return Err(ApiError::new(
                ApiErrorCode::InvalidArgument,
                "at most 256 selected scopes",
            ));
        }
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let revision = self.config_revision.load(Ordering::Relaxed);
        if query
            .expected_core_instance_id
            .as_deref()
            .is_some_and(|expected| expected != core)
            || query
                .expected_config_revision
                .is_some_and(|expected| expected != revision)
        {
            return Err(ApiError::new(
                ApiErrorCode::Conflict,
                "statistics query instance or configuration changed",
            ));
        }
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        if query
            .expected_registry_revision
            .is_some_and(|r| r != self.revision.load(Ordering::Relaxed))
        {
            return Err(ApiError::new(
                ApiErrorCode::Conflict,
                "statistics registry changed; restart pagination",
            ));
        }
        if query.scopes.iter().any(|s| !entries.contains_key(s)) {
            return Err(ApiError::new(
                ApiErrorCode::NotFound,
                "selected statistics scope no longer exists",
            ));
        }
        let limit = query.limit.unwrap_or(64).clamp(1, 256);
        let selected = entries
            .iter()
            .filter(|(scope, _)| query.scopes.is_empty() || query.scopes.contains(scope));
        let total = selected.clone().count();
        let scopes = selected
            .skip(query.offset)
            .take(limit)
            .map(|(scope, entry)| {
                let period = entry.period.lock().unwrap_or_else(|e| e.into_inner());
                project(scope, entry, &period, core, revision)
            })
            .collect::<Vec<_>>();
        let end = query.offset.saturating_add(scopes.len());
        Ok(TrafficListSnapshot {
            core_instance_id: core.into(),
            config_revision: revision,
            registry_revision: self.revision.load(Ordering::Relaxed),
            sampled_at_unix_ms: now(),
            scopes,
            total,
            next_offset: (end < total).then_some(end),
        })
    }
}
pub(super) fn project(
    scope: &TrafficScope,
    entry: &CounterSet,
    period: &Period,
    core: &str,
    revision: u64,
) -> TrafficSnapshot {
    let started = now();
    let values = entry.capture();
    let mut planes = [TrafficPlane::Flow, TrafficPlane::Inner, TrafficPlane::Outer]
        .into_iter()
        .map(|plane| {
            entry.planes[plane as usize].project(
                plane,
                values[plane as usize],
                period.baseline[plane as usize],
            )
        })
        .collect::<Vec<_>>();
    planes[0].accounting_basis = match scope {
        TrafficScope::Global => "business_direction_max_mirrored_boundaries",
        TrafficScope::Inbound { .. } => "inbound_business_boundary",
        TrafficScope::Outbound { .. } => "final_outbound_flow_observation_not_hop_carrier",
        TrafficScope::Endpoint { .. } => "business_direction_deduplicated_endpoint_roles",
        TrafficScope::Peer { .. } if planes[0].available_metrics.is_empty() => "unavailable",
        TrafficScope::Peer { .. } => "business_direction_deduplicated_protocol_peer_flow_bindings",
    }
    .into();
    TrafficSnapshot {
        scope: scope.clone(),
        core_instance_id: core.into(),
        config_revision: revision,
        generation: *entry.generation.lock().unwrap_or_else(|e| e.into_inner()),
        stats_epoch: period.epoch.clone(),
        epoch_started_at_unix_ms: period.started,
        capture_started_at_unix_ms: started,
        sampled_at_monotonic_ns: entry.clock.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        sampled_at_unix_ms: now(),
        planes,
        activity: TrafficActivity {
            active_stream_flows: (entry.flows_available.load(Ordering::Acquire) != 0)
                .then(|| entry.active_streams.load(Ordering::Relaxed)),
            active_datagram_flows: (entry.flows_available.load(Ordering::Acquire) != 0)
                .then(|| entry.active_datagrams.load(Ordering::Relaxed)),
            active_packet_routes: (entry.routes_available.load(Ordering::Acquire) != 0)
                .then(|| entry.active_routes.load(Ordering::Relaxed)),
        },
        reset_policy: "all_available_cumulative_no_cascade".into(),
    }
}

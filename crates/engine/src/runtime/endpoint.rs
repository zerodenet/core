//! Configuration-owned endpoint identity. Runtime facts are injected by Proxy.

use super::Engine;
use zero_api::{EndpointGetQuery, EndpointListQuery, EndpointListSnapshot, EndpointSnapshot};

mod admission;
mod facts;
mod flows;
mod intent;
mod management;
pub use admission::EndpointAdmission;
pub(super) use facts::EndpointFacts;
pub(crate) use facts::Fact;
pub(super) use intent::EndpointIntents;
pub use management::EndpointChange;

impl super::EngineRuntimeSnapshot {
    pub(crate) fn admit_endpoint_outbound<'a>(
        &self,
        resolved: crate::ResolvedOutbound<'a>,
    ) -> Result<crate::ResolvedOutbound<'a>, crate::EngineError> {
        let check = |leaf: &crate::ResolvedLeafOutbound<'_>| {
            let crate::ResolvedLeafOutbound::Proxy { identity } = leaf else {
                return Ok(());
            };
            let Some(outbound) = self.config().outbounds.get(identity.config_index()) else {
                return Ok(());
            };
            match EndpointAdmission::from_snapshot(self).outbound_denial(&outbound.tag) {
                Some((reason, endpoint_id)) => Err(crate::EngineError::InvalidPlan {
                    message: format!("{reason}: {endpoint_id}"),
                }),
                None => Ok(()),
            }
        };
        match resolved {
            crate::ResolvedOutbound::Single(leaf) => {
                check(&leaf)?;
                Ok(crate::ResolvedOutbound::Single(leaf))
            }
            crate::ResolvedOutbound::Relay { chain } => {
                for leaf in &chain {
                    check(leaf)?;
                }
                Ok(crate::ResolvedOutbound::Relay { chain })
            }
            crate::ResolvedOutbound::Fallback { candidates } => {
                let mut admitted = Vec::new();
                let mut denied = None;
                for leaf in candidates {
                    match check(&leaf) {
                        Ok(()) => admitted.push(leaf),
                        Err(error) => denied = Some(error),
                    }
                }
                if admitted.is_empty() {
                    if let Some(error) = denied {
                        return Err(error);
                    }
                }
                Ok(crate::ResolvedOutbound::Fallback {
                    candidates: admitted,
                })
            }
        }
    }
}

impl Engine {
    pub fn endpoint_inbound_allowed(&self, tag: &str) -> bool {
        EndpointAdmission::from_snapshot(&self.runtime_snapshot()).inbound_allowed(tag)
    }

    pub fn endpoints_snapshot(&self, query: &EndpointListQuery) -> EndpointListSnapshot {
        let snapshot = self.runtime_snapshot();
        self.endpoints_snapshot_in(&snapshot, query)
    }

    pub fn endpoints_snapshot_in(
        &self,
        snapshot: &super::EngineRuntimeSnapshot,
        query: &EndpointListQuery,
    ) -> EndpointListSnapshot {
        let bindings = snapshot.config().endpoint_bindings();
        let total = bindings.len();
        let limit = query.limit.unwrap_or(100).clamp(1, 1000);
        let endpoints = bindings
            .into_iter()
            .skip(query.offset)
            .take(limit)
            .map(|binding| {
                let intent = &snapshot.endpoint_intents.entries[&binding.endpoint_id];
                let (stream, datagram) = self.endpoint_active_flow_counts(&binding);
                let mut endpoint = EndpointSnapshot {
                    endpoint_id: binding.endpoint_id,
                    tag: binding.tag,
                    protocol: binding.protocol,
                    inbound_tags: binding.inbound_tags,
                    outbound_tags: binding.outbound_tags,
                    enabled: intent.enabled(),
                    allowed: intent.directions(),
                    intent_revision: intent.revision,
                    state_source: intent.source(),
                    core_instance_id: self.core_instance_id().to_owned(),
                    config_revision: snapshot.config_revision(),
                    observed_at_unix_ms: super::started_at_unix_ms(),
                    counters: zero_api::EndpointCounters {
                        active_stream_flows: Some(stream),
                        active_datagram_flows: Some(datagram),
                        ..Default::default()
                    },
                    ..Default::default()
                };
                self.endpoint_facts.project(&mut endpoint);
                endpoint
            })
            .collect::<Vec<_>>();
        let end = query.offset.saturating_add(endpoints.len());
        EndpointListSnapshot {
            endpoints,
            total,
            next_offset: (end < total).then_some(end),
        }
    }

    pub fn endpoint_snapshot(
        &self,
        query: &EndpointGetQuery,
    ) -> zero_api::ApiResult<EndpointSnapshot> {
        let snapshot = self.runtime_snapshot();
        self.endpoint_snapshot_in(&snapshot, query)
    }

    pub fn endpoint_snapshot_in(
        &self,
        snapshot: &super::EngineRuntimeSnapshot,
        query: &EndpointGetQuery,
    ) -> zero_api::ApiResult<EndpointSnapshot> {
        let binding = snapshot
            .config()
            .endpoint_bindings()
            .into_iter()
            .find(|binding| binding.endpoint_id == query.endpoint_id)
            .ok_or_else(|| {
                zero_api::ApiError::new(
                    zero_api::ApiErrorCode::NotFound,
                    format!("endpoint `{}` was not found", query.endpoint_id),
                )
            })?;
        let intent = &snapshot.endpoint_intents.entries[&binding.endpoint_id];
        let (stream, datagram) = self.endpoint_active_flow_counts(&binding);
        let mut endpoint = EndpointSnapshot {
            endpoint_id: binding.endpoint_id,
            tag: binding.tag,
            protocol: binding.protocol,
            inbound_tags: binding.inbound_tags,
            outbound_tags: binding.outbound_tags,
            enabled: intent.enabled(),
            allowed: intent.directions(),
            intent_revision: intent.revision,
            state_source: intent.source(),
            core_instance_id: self.core_instance_id().to_owned(),
            config_revision: snapshot.config_revision(),
            observed_at_unix_ms: super::started_at_unix_ms(),
            counters: zero_api::EndpointCounters {
                active_stream_flows: Some(stream),
                active_datagram_flows: Some(datagram),
                ..Default::default()
            },
            ..Default::default()
        };
        self.endpoint_facts.project(&mut endpoint);
        Ok(endpoint)
    }

    /// Proxy reports an applied resource fact only after listener/device
    /// reconciliation has completed. The engine owns its generation and event.
    pub fn record_endpoint_runtime_state(&self, endpoint: &EndpointSnapshot) {
        if let Some(changed) = self.endpoint_facts.record(endpoint) {
            self.event_log.push_endpoint_state_changed(&changed);
        }
    }

    pub fn record_endpoint_runtime_error(&self, id: &str, message: &str, failed: bool) {
        if let Some(changed) = self.endpoint_facts.record_error(id, message, failed) {
            self.event_log.push_endpoint_state_changed(&changed);
        }
    }
}

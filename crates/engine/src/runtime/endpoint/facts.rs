//! Applied runtime facts; configuration intent remains in immutable snapshots.

use std::{collections::BTreeMap, sync::Mutex};

use zero_api::{EndpointRuntimeState, EndpointSnapshot, ErrorDetail};

#[derive(Debug, Clone)]
pub(crate) struct Fact {
    pub endpoint_id: String,
    pub state: EndpointRuntimeState,
    pub generation: u64,
    pub started_at_unix_ms: Option<u64>,
    pub intent_revision: u64,
    pub enabled: bool,
    pub last_error: Option<ErrorDetail>,
    incarnations: Vec<u64>,
    pub recovery: Option<zero_api::EndpointRecovery>,
}

#[derive(Default, Debug)]
struct State {
    next_generation: u64,
    entries: BTreeMap<String, Fact>,
}

#[derive(Default, Debug)]
pub(crate) struct EndpointFacts(Mutex<State>);

impl EndpointFacts {
    pub fn remove(&self, id: &str) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .remove(id);
    }

    pub fn confirms(&self, id: &str, revision: u64) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(id)
            .is_some_and(|fact| {
                fact.intent_revision == revision
                    && matches!(
                        fact.state,
                        EndpointRuntimeState::Running | EndpointRuntimeState::Stopped
                    )
            })
    }
    pub fn project(&self, endpoint: &mut EndpointSnapshot) {
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(fact) = state.entries.get(&endpoint.endpoint_id) {
            endpoint.state = fact.state;
            endpoint.generation = (fact.generation > 0).then_some(fact.generation);
            endpoint.started_at_unix_ms = fact.started_at_unix_ms;
            endpoint.last_error = fact.last_error.clone();
            endpoint.recovery = fact.recovery.clone();
        }
    }

    pub fn record(&self, endpoint: &EndpointSnapshot) -> Option<Fact> {
        self.record_device(endpoint, None)
    }
    pub fn record_device(
        &self,
        endpoint: &EndpointSnapshot,
        incarnations: Option<Vec<u64>>,
    ) -> Option<Fact> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let previous = state.entries.get(&endpoint.endpoint_id).cloned();
        let rebuilt = incarnations.as_ref().is_some_and(|ids| {
            !ids.is_empty()
                && previous
                    .as_ref()
                    .is_some_and(|fact| !fact.incarnations.is_empty() && fact.incarnations != *ids)
        });
        let generation = if endpoint.state == EndpointRuntimeState::Running
            && (rebuilt
                || previous
                    .as_ref()
                    .is_none_or(|fact| fact.started_at_unix_ms.is_none()))
        {
            state.next_generation = state.next_generation.saturating_add(1);
            state.next_generation
        } else {
            previous.as_ref().map_or(0, |fact| fact.generation)
        };
        let started_at_unix_ms = if endpoint.state == EndpointRuntimeState::Running {
            previous
                .as_ref()
                .filter(|_| !rebuilt)
                .and_then(|fact| fact.started_at_unix_ms)
                .or(Some(endpoint.observed_at_unix_ms))
        } else if matches!(
            endpoint.state,
            EndpointRuntimeState::Starting | EndpointRuntimeState::Stopping
        ) {
            // Transitional observation does not rebuild a live resource.
            previous.as_ref().and_then(|fact| fact.started_at_unix_ms)
        } else {
            None
        };
        let fact = Fact {
            endpoint_id: endpoint.endpoint_id.clone(),
            state: endpoint.state,
            generation,
            started_at_unix_ms,
            intent_revision: endpoint.intent_revision,
            enabled: endpoint.enabled,
            last_error: None,
            incarnations: incarnations.unwrap_or_else(|| {
                previous
                    .as_ref()
                    .map(|f| f.incarnations.clone())
                    .unwrap_or_default()
            }),
            recovery: matches!(
                endpoint.state,
                EndpointRuntimeState::Running
                    | EndpointRuntimeState::Starting
                    | EndpointRuntimeState::Stopping
            )
            .then(|| previous.as_ref().and_then(|f| f.recovery.clone()))
            .flatten(),
        };
        let changed = previous.as_ref().is_none_or(|old| {
            old.state != fact.state
                || old.generation != fact.generation
                || old.intent_revision != fact.intent_revision
                || old.enabled != fact.enabled
                || old.last_error != fact.last_error
        });
        state.entries.insert(fact.endpoint_id.clone(), fact.clone());
        changed.then_some(fact)
    }

    pub fn record_recovery(&self, id: &str, recovery: zero_api::EndpointRecovery) -> Option<Fact> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let fact = state.entries.get_mut(id)?;
        fact.recovery = Some(recovery);
        Some(fact.clone())
    }

    pub fn record_error(&self, id: &str, message: &str, failed: bool) -> Option<Fact> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let fact = state.entries.get_mut(id)?;
        fact.last_error = Some(ErrorDetail::new(None::<String>, message));
        if failed {
            fact.state = EndpointRuntimeState::Failed;
            fact.started_at_unix_ms = None;
            fact.recovery = None;
        }
        Some(fact.clone())
    }
}

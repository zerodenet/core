use std::collections::HashMap;

use zero_api::PrincipalFlowSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrincipalFlowObservation {
    pub(crate) active_flows: u64,
    pub(crate) session_registry_revision: u64,
    pub(crate) observed_at_unix_ms: u64,
}

#[derive(Debug, Default)]
pub(super) struct PrincipalFlowIndex {
    revision: u64,
    active: HashMap<String, PrincipalFlowState>,
}

#[derive(Debug)]
struct PrincipalFlowState {
    active_flows: u64,
    last_transition_revision: u64,
    observed_at_unix_ms: u64,
}

impl PrincipalFlowIndex {
    pub(super) fn started(
        &mut self,
        principal_key: Option<&str>,
        observed_at_unix_ms: u64,
    ) -> Option<PrincipalFlowObservation> {
        let principal_key = principal_key?;
        let revision = self.next_revision();
        let state = self
            .active
            .entry(principal_key.to_owned())
            .or_insert(PrincipalFlowState {
                active_flows: 0,
                last_transition_revision: revision,
                observed_at_unix_ms,
            });
        state.active_flows = state
            .active_flows
            .checked_add(1)
            .expect("principal active flow count overflowed");
        state.last_transition_revision = revision;
        state.observed_at_unix_ms = observed_at_unix_ms;
        Some(PrincipalFlowObservation {
            active_flows: state.active_flows,
            session_registry_revision: revision,
            observed_at_unix_ms,
        })
    }

    pub(super) fn completed(
        &mut self,
        principal_key: Option<&str>,
        observed_at_unix_ms: u64,
    ) -> Option<PrincipalFlowObservation> {
        let principal_key = principal_key?;
        let revision = self.next_revision();
        let active_flows = {
            let state = self
                .active
                .get_mut(principal_key)
                .expect("principal flow index must contain an active session");
            state.active_flows = state
                .active_flows
                .checked_sub(1)
                .expect("principal active flow count underflowed");
            state.last_transition_revision = revision;
            state.observed_at_unix_ms = observed_at_unix_ms;
            state.active_flows
        };
        if active_flows == 0 {
            self.active.remove(principal_key);
        }
        Some(PrincipalFlowObservation {
            active_flows,
            session_registry_revision: revision,
            observed_at_unix_ms,
        })
    }

    pub(super) fn snapshot(&self) -> (u64, Vec<PrincipalFlowSnapshot>) {
        let mut principals = self
            .active
            .iter()
            .map(|(principal_key, state)| PrincipalFlowSnapshot {
                principal_key: principal_key.clone(),
                active_flows: state.active_flows,
                last_transition_revision: state.last_transition_revision,
                observed_at_unix_ms: state.observed_at_unix_ms,
            })
            .collect::<Vec<_>>();
        principals.sort_by(|left, right| left.principal_key.cmp(&right.principal_key));
        (self.revision, principals)
    }

    fn next_revision(&mut self) -> u64 {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("session registry revision overflowed");
        self.revision
    }
}

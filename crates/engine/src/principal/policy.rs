//! Principal policy revision and admission state.

use std::collections::HashMap;
use std::sync::RwLock;

use zero_config::RuntimeConfig;
use zero_core::SessionAuth;

use crate::EngineError;

#[derive(Debug, Default)]
pub(crate) struct PrincipalPolicyRegistry {
    policies: RwLock<HashMap<String, PrincipalPolicyState>>,
}

#[derive(Debug, Clone, Copy)]
enum PrincipalPolicyState {
    Active(u64),
    Disabled,
}

impl PrincipalPolicyRegistry {
    pub(crate) fn from_config(config: &RuntimeConfig) -> Self {
        let registry = Self::default();
        registry.replace_from_config(config);
        registry
    }

    pub(crate) fn replace_from_config(&self, config: &RuntimeConfig) {
        let mut revisions = HashMap::new();
        for inbound in &config.inbounds {
            collect_revisions(
                &mut revisions,
                inbound.protocol.principal_policy_revisions().into_iter(),
            );
        }
        let mut policies = self
            .policies
            .write()
            .unwrap_or_else(|error| error.into_inner());
        for principal_key in policies.keys().cloned().collect::<Vec<_>>() {
            revisions
                .entry(principal_key)
                .or_insert(PrincipalPolicyState::Disabled);
        }
        *policies = revisions;
    }

    pub(crate) fn validate(&self, auth: Option<&SessionAuth>) -> Result<(), EngineError> {
        let Some(auth) = auth else {
            return Ok(());
        };
        let Some(principal_key) = auth.principal_key.as_deref() else {
            return Ok(());
        };
        let current = self
            .policies
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(principal_key)
            .copied();
        let Some(current) = current else {
            return Ok(());
        };
        let PrincipalPolicyState::Active(current) = current else {
            return Err(EngineError::AdmissionDenied {
                reason: format!("principal `{principal_key}` is disabled"),
            });
        };
        if auth.policy_revision == Some(current) {
            return Ok(());
        }
        Err(EngineError::AdmissionDenied {
            reason: format!(
                "principal `{principal_key}` authenticated with stale policy revision {:?}; current revision is {current}",
                auth.policy_revision
            ),
        })
    }
}

fn collect_revisions<'a>(
    revisions: &mut HashMap<String, PrincipalPolicyState>,
    principals: impl Iterator<Item = (&'a str, u64)>,
) {
    for (principal_key, revision) in principals {
        revisions
            .entry(principal_key.to_owned())
            .and_modify(|current| {
                if let PrincipalPolicyState::Active(current_revision) = current {
                    *current_revision = (*current_revision).max(revision);
                } else {
                    *current = PrincipalPolicyState::Active(revision);
                }
            })
            .or_insert(PrincipalPolicyState::Active(revision));
    }
}

//! Every Bound declaration holds its one actual provider before any factory IO.
use super::*;
use crate::component::bound_datagram::{
    HostBoundDatagramResources, evidence::BoundDatagramEvidence,
};
use zero_core::component::{ComponentInstanceId, NetworkGeneration};
use zero_platform_tokio::contract::{
    component::{DatagramBindingAuthorization, DatagramBindingId},
    scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity},
};
mod close;
struct Entry {
    provider: Arc<HostBoundDatagramResources>,
    scope: TaskScopeIdentity,
    binding: std::sync::OnceLock<DatagramBindingId>,
}
pub(super) struct BoundDeclarations {
    component: ComponentId,
    entries: BTreeMap<usize, Entry>,
}
impl BoundDeclarations {
    pub(super) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let mut entries = BTreeMap::new();
        for (ordinal, declaration) in declarations.iter().enumerate() {
            if !matches!(
                declaration,
                ResourceDeclaration::BoundOutboundDatagram { .. }
            ) {
                continue;
            }
            let (instance, scope) = identity.as_ref().ok_or_else(|| {
                error(
                    component,
                    Reason::InvalidConfig,
                    "Bound UDP requires its actual host instance and Scope",
                )
            })?;
            entries.insert(
                ordinal,
                Entry {
                    provider: HostBoundDatagramResources::new_scoped(
                        *instance,
                        scope.clone(),
                        declaration,
                        cancel.clone(),
                    )?,
                    scope: scope.clone(),
                    binding: std::sync::OnceLock::new(),
                },
            );
        }
        Ok(Self { component, entries })
    }
    pub(super) fn origins(
        &self,
        ordinal: usize,
    ) -> Vec<zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin> {
        self.entries
            .get(&ordinal)
            .map(|entry| entry.provider.actual_origins())
            .unwrap_or_default()
    }
    pub(super) fn activate(
        &self,
        ordinal: usize,
        witness: &TaskJoinWitness,
    ) -> Result<DatagramBindingId, ChannelError> {
        let entry = self.entries.get(&ordinal).ok_or_else(|| {
            error(
                self.component,
                Reason::InvalidConfig,
                "no such actual Bound UDP declaration",
            )
        })?;
        let binding = entry
            .binding
            .get()
            .ok_or_else(|| {
                error(
                    self.component,
                    Reason::Revoked,
                    "host has not issued this Bound UDP source",
                )
            })?
            .clone();
        entry.provider.activate_driver(witness)?;
        Ok(binding)
    }
    pub(super) fn owner_for(
        &self,
        binding: &DatagramBindingId,
    ) -> Result<&HostBoundDatagramResources, ChannelError> {
        self.entries
            .values()
            .find(|entry| {
                entry.provider.owns_binding(binding) && entry.binding.get() == Some(binding)
            })
            .map(|entry| entry.provider.as_ref())
            .ok_or_else(|| {
                error(
                    self.component,
                    Reason::StalePlan,
                    "Bound UDP binding differs from this actual host provider",
                )
            })
    }
    pub(super) fn evidence(&self) -> Vec<BoundDatagramEvidence> {
        self.entries
            .iter()
            .map(|(ordinal, entry)| entry.provider.evidence(*ordinal))
            .collect()
    }
}
impl HostResources {
    pub(in crate::component) fn bound_datagram_origin_available(&self, ordinal: usize) -> bool {
        !self.bounds.origins(ordinal).is_empty()
    }
    pub(in crate::component) fn owns_bound_datagram_origin(
        &self,
        ordinal: usize,
        actual: &zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
    ) -> bool {
        self.bounds
            .origins(ordinal)
            .iter()
            .any(|origin| origin.same_original(actual))
    }
    /// Host assembly only: ResourceAccess cannot mint source policy or binding.
    /// Called for the same cold provider before its lifecycle's start phase.
    pub fn issue_bound_datagram_binding(
        &self,
        ordinal: usize,
        source: Arc<dyn DatagramBindingAuthorization>,
        expected: NetworkGeneration,
        tasks: &TaskRegistrar,
    ) -> Result<DatagramBindingId, ChannelError> {
        let entry = self
            .bounds
            .entries
            .get(&ordinal)
            .ok_or_else(|| self.denied())?;
        if !entry.scope.matches(tasks) || entry.binding.get().is_some() {
            return Err(self.denied());
        }
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed || state.start_deadline.is_some() || self.cancel.is_cancelled() {
                return Err(self.denied());
            }
        }
        let binding = entry.provider.authorize(source, expected)?;
        entry
            .binding
            .set(binding.clone())
            .map_err(|_| self.denied())?;
        Ok(binding)
    }
}

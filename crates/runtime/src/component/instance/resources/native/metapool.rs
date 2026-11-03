//! Same real on-demand ancillary owner in the canonical declaration ordinal.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::scope::{TaskRegistrar, TaskScopeIdentity},
    controlled_datagram::metadata_pool::{MetadataPoolOwnerClosed, MetadataPoolRegistration},
};
pub(super) struct MetadataPools {
    identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    declarations: BTreeMap<usize, ResourceDeclaration>,
    records: Mutex<BTreeMap<usize, Record>>,
    cancel: Arc<CancellationToken>,
}
struct Record {
    actual: MetadataPoolRegistration,
    bound: bool,
    failure: Option<Arc<ChannelError>>,
}
#[derive(Clone)]
pub(in crate::component) struct MetadataPoolEvidence {
    pub ordinal: usize,
    actual: Option<MetadataPoolRegistration>,
    closed: Option<MetadataPoolOwnerClosed>,
    bound: bool,
    pub first_fault: Option<Arc<ChannelError>>,
}
impl std::fmt::Debug for MetadataPoolEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataPoolEvidence")
            .field("ordinal", &self.ordinal)
            .field("confirmed", &self.physical_confirmed())
            .finish_non_exhaustive()
    }
}
impl MetadataPoolEvidence {
    pub(super) fn physical_confirmed(&self) -> bool {
        self.bound
            && self
                .actual
                .as_ref()
                .zip(self.closed.as_ref())
                .is_some_and(|(actual, closed)| closed.matches(actual))
    }
}
impl MetadataPools {
    pub(super) fn original_registration(&self, ordinal: usize) -> Option<MetadataPoolRegistration> {
        self.records
            .lock()
            .expect("metadata registrations poisoned")
            .get(&ordinal)
            .filter(|record| record.bound)
            .map(|record| record.actual.clone())
    }
    pub(super) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let declarations: BTreeMap<_, _> = declarations
            .iter()
            .enumerate()
            .filter(|(_, d)| {
                matches!(
                    d,
                    ResourceDeclaration::ControlledMetadataDatagram { .. }
                        | ResourceDeclaration::ControlledMetadataCarrier { .. }
                )
            })
            .map(|(n, d)| (n, d.clone()))
            .collect();
        if !declarations.is_empty() && identity.is_none() {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "metadata pools require an actual host instance/scope",
            ));
        }
        Ok(Self {
            identity,
            declarations,
            records: Mutex::default(),
            cancel: Arc::new(cancel),
        })
    }
    pub(super) fn revoke(&self) {
        let records = self
            .records
            .lock()
            .expect("metadata registrations poisoned")
            .values()
            .map(|r| r.actual.clone())
            .collect::<Vec<_>>();
        for actual in records {
            actual.revoke();
        }
    }
    pub(super) fn evidence(&self) -> Vec<MetadataPoolEvidence> {
        let records = self
            .records
            .lock()
            .expect("metadata registrations poisoned");
        self.declarations
            .keys()
            .map(|ordinal| {
                let record = records.get(ordinal);
                MetadataPoolEvidence {
                    ordinal: *ordinal,
                    actual: record.map(|r| r.actual.clone()),
                    closed: record.and_then(|r| r.actual.closed()),
                    bound: record.is_some_and(|r| r.bound),
                    first_fault: record.and_then(|r| r.failure.clone()),
                }
            })
            .collect()
    }
}
impl HostResources {
    pub(super) fn register_metapool(
        &self,
        ordinal: usize,
        actual: MetadataPoolRegistration,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        let (instance, scope) = self
            .metapools
            .identity
            .as_ref()
            .ok_or_else(|| self.denied())?;
        let declaration = self
            .metapools
            .declarations
            .get(&ordinal)
            .ok_or_else(|| self.denied())?;
        if actual.instance() != *instance || !scope.matches(tasks) || !scope.resource_open() {
            return Err(self.denied());
        }
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.listener_phase(&state)?;
            let mut records = self
                .metapools
                .records
                .lock()
                .expect("metadata registrations poisoned");
            if records
                .iter()
                .any(|(n, r)| *n != ordinal && r.actual.same_owner(&actual))
            {
                return Err(self.denied());
            }
            if let Some(record) = records.get(&ordinal) {
                if !record.actual.same_owner(&actual) {
                    return Err(self.denied());
                }
                if record.bound {
                    return Ok(());
                }
            } else {
                records.insert(
                    ordinal,
                    Record {
                        actual: actual.clone(),
                        bound: false,
                        failure: None,
                    },
                );
            }
        }
        let result = actual.bind_declared(declaration, tasks, &self.metapools.cancel);
        let result = result.and_then(|()| {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.listener_phase(&state)?;
            if !actual.matches_binding(declaration, tasks, &self.metapools.cancel) {
                return Err(self.denied());
            }
            Ok(())
        });
        let mut records = self
            .metapools
            .records
            .lock()
            .expect("metadata registrations poisoned");
        let record = records
            .get_mut(&ordinal)
            .expect("original registration persisted");
        match result {
            Ok(()) => {
                record.bound = true;
                Ok(())
            }
            Err(source) => {
                let original = record
                    .failure
                    .get_or_insert_with(|| Arc::new(source))
                    .clone();
                drop(records);
                self.cancel.cancel();
                Err(crate::component::instance::retained(original))
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/resource_metapool.rs"]
mod tests;

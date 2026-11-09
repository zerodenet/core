//! Actual fault projection and handoff from the same charged Root rows.
use super::*;
impl CarrierOriginalTotals {
    /// Same retained custody rows, not a native/Scope-close assertion.
    pub fn retained_for_instance(
        &self,
        instance: zero_core::component::ComponentInstanceId,
    ) -> usize {
        self.reconcile();
        let state = self.lock();
        state
            .originals
            .values()
            .filter(|row| row.instance == Some(instance))
            .count()
            + state
                .metadata
                .iter()
                .filter(|row| row.instance == Some(instance))
                .count()
            + state
                .bound
                .iter()
                .filter(|row| row.instance == Some(instance))
                .count()
            + state
                .source
                .iter()
                .filter(|row| row.instance == Some(instance))
                .count()
    }
    /// Actual typed resource faults from these same retained rows. Unknown
    /// instance remains visible through failures_with_instances and cannot ACK.
    pub fn resource_failures(&self) -> Vec<Arc<crate::contract::lifecycle::ResourceFailure>> {
        let metadata = self.lock().metadata.clone();
        let bound = self.lock().bound.clone();
        let source = self.lock().source.clone();
        self.originals()
            .into_iter()
            .filter_map(|(_, original)| {
                original.observe(original.failure(), original.resource_failure())
            })
            .chain(metadata.into_iter().filter_map(|original| {
                original.observe(original.failure(), original.resource_failure())
            }))
            .chain(bound.into_iter().filter_map(|original| {
                original.observe(original.failure(), original.resource_failure())
            }))
            .chain(source.into_iter().filter_map(|original| {
                original.observe(original.failure(), original.resource_failure())
            }))
            .collect()
    }
    /// Original typed failures stay owned even if the protocol Owner is lost.
    /// This does not accept an error on behalf of its actual task/receiver.
    pub fn failures(&self) -> Vec<crate::contract::RetainedError> {
        self.failures_with_instances()
            .into_iter()
            .map(|actual| actual.source)
            .collect()
    }
    /// Read only. An unknown original instance cannot be attributed to a current
    /// Slot or converted to a successful no-acquisition/retirement observation.
    pub fn failures_with_instances(&self) -> Vec<CarrierOriginalFailure> {
        let metadata = self.lock().metadata.clone();
        let bound = self.lock().bound.clone();
        let source = self.lock().source.clone();
        self.originals()
            .into_iter()
            .filter_map(|(_, original)| {
                original.failure().map(|source| CarrierOriginalFailure {
                    instance: original.instance(),
                    source: source.into(),
                })
            })
            .chain(metadata.into_iter().filter_map(|original| {
                original.failure().map(|source| CarrierOriginalFailure {
                    instance: original.instance(),
                    source: source.into(),
                })
            }))
            .chain(bound.into_iter().filter_map(|original| {
                original.failure().map(|source| CarrierOriginalFailure {
                    instance: original.instance(),
                    source: source.into(),
                })
            }))
            .chain(source.into_iter().filter_map(|original| {
                original.failure().map(|source| CarrierOriginalFailure {
                    instance: original.instance(),
                    source: source.into(),
                })
            }))
            .collect()
    }
}

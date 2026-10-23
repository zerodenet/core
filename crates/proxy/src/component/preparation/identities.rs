//! Keep lightweight canonical IDs, never old factory/Options/runtime owners.
use super::{Key, ResourcePreparation};
use std::collections::BTreeMap;
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::lifecycle::BuildError;

#[derive(Clone, Default)]
pub struct SharedResourceIdentities {
    pub(super) entries: BTreeMap<Key, ComponentId>,
}

impl<'a> ResourcePreparation<'a> {
    /// Continue one actual host allocation domain and compiled factory assembly.
    /// The host retains monotonic allocation; namespaces identify the same owning
    /// state across passes. This metadata does not retain any old runtime owner.
    pub fn with_identities(
        allocate: &'a mut dyn FnMut() -> Result<ComponentId, BuildError>,
        identities: &SharedResourceIdentities,
    ) -> Self {
        let mut preparation = Self::new(allocate);
        preparation.previous = identities.clone();
        preparation
    }

    /// Only resources requested by this successful candidate remain in its
    /// identity index. A removed/re-added resource gets a new host identity.
    /// The host publishes this snapshot with the confirmed candidate only;
    /// failed or discarded candidates cannot replace its active identity index.
    pub fn identities(&self) -> Result<SharedResourceIdentities, BuildError> {
        self.ensure_valid()?;
        Ok(SharedResourceIdentities {
            entries: self
                .resources
                .iter()
                .map(|(key, resource)| (key.clone(), resource.component))
                .collect(),
        })
    }
}

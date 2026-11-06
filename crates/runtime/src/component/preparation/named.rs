//! Actual primary IDs reserved once by the same cold catalog pass.
use super::*;
impl ResourcePreparation<'_> {
    pub(in crate::component) fn reserve_primary_components(
        &mut self,
        builtin: Option<ComponentId>,
        named: impl IntoIterator<Item = (String, Option<ComponentId>)>,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if self.builtin_component.is_some() || !self.named_components.is_empty() {
            return Err(invalid(
                Reason::StalePlan,
                "primary identities already reserved",
            ));
        }
        self.builtin_component = Some(match builtin {
            Some(id) => id,
            None => (self.allocate)()?,
        });
        for (tag, old) in named {
            let id = match old {
                Some(id) => id,
                None => (self.allocate)()?,
            };
            if self.named_components.insert(tag, id).is_some() {
                self.failed.set(true);
                return Err(invalid(
                    Reason::InvalidConfig,
                    "duplicate primary component name",
                ));
            }
        }
        Ok(())
    }
    /// This pass's real named component ID, including a forward dependency.
    /// No generation, material, build/admission or native authority is inferred.
    pub fn named_component(&self, tag: &str) -> Option<ComponentId> {
        self.ensure_valid().ok()?;
        self.named_components.get(tag).copied()
    }
    pub fn builtin_component(&self) -> Option<ComponentId> {
        self.ensure_valid().ok()?;
        self.builtin_component
    }
}

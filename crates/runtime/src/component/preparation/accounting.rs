//! One candidate's finite Host accounting projection, without configuration retention.
use super::*;

impl ResourcePreparation<'_> {
    pub(in crate::component) fn project_carrier_total(
        &self,
        recipe: ComponentRecipe,
    ) -> Result<ComponentRecipe, BuildError> {
        let limit = self.carrier_total_limit_for(recipe.spec())?;
        Ok(recipe.with_carrier_total_limit(limit))
    }
    pub(super) fn carrier_total_limit_for(&self, spec: &ComponentSpec) -> Result<Option<usize>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        let limit = candidate.runtime.effective_carrier_max_total_originals();
        if !(1..=65536).contains(&limit) {
            return Err(invalid(
                Reason::InvalidConfig,
                "invalid actual carrier total capacity",
            ));
        }
        Ok(crate::component::recipe::accounting::owns_carrier_custody(spec).then_some(limit))
    }
}

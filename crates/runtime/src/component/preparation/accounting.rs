//! One candidate's finite Host accounting projection, without configuration retention.
use super::*;

impl ResourcePreparation<'_> {
    pub(in crate::component) fn project_carrier_total(
        &self,
        recipe: ComponentRecipe,
    ) -> Result<ComponentRecipe, BuildError> {
        let limit = self.carrier_total_limit_for(recipe.spec())?;
        let process = self.process_limits_for(recipe.spec())?;
        let preparation = self.preparation_limits_for(recipe.spec())?;
        Ok(recipe
            .with_carrier_total_limit(limit)
            .with_process_limits(process)
            .with_preparation_limits(preparation))
    }
    pub(super) fn carrier_total_limit_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<usize>, BuildError> {
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

impl ResourcePreparation<'_> {
    pub(super) fn process_limits_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<crate::component::recipe::accounting::ProcessLimits>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        if !crate::component::recipe::accounting::owns_process_custody(spec) {
            return Ok(None);
        }
        Ok(Some(
            crate::component::recipe::accounting::ProcessLimits::from_runtime(&candidate.runtime)?,
        ))
    }
}

impl ResourcePreparation<'_> {
    pub(super) fn preparation_limits_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<crate::component::recipe::accounting::PreparationLimits>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        if !crate::component::recipe::accounting::owns_carrier_custody(spec) {
            return Ok(None);
        }
        crate::component::recipe::accounting::PreparationLimits::from_runtime(&candidate.runtime)
            .map(Some)
    }
}
